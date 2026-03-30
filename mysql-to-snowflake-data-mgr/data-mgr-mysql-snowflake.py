import os
import csv
import uuid
import hashlib
import logging
from pathlib import Path
from decimal import Decimal
from datetime import datetime, timezone

import pandas as pd
import mysql.connector
import psycopg2
from psycopg2.extras import execute_values


# =========================================================
# CONFIG
# =========================================================

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "your-mysql-host"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "your_mysql_user"),
    "password": os.getenv("MYSQL_PASSWORD", "your_mysql_password"),
    "database": os.getenv("MYSQL_DATABASE", "your_mysql_database"),
}

PG_CONFIG = {
    "host": os.getenv("PG_HOST", "your-aurora-postgres-host"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "user": os.getenv("PG_USER", "your_pg_user"),
    "password": os.getenv("PG_PASSWORD", "your_pg_password"),
    "dbname": os.getenv("PG_DATABASE", "your_pg_database"),
    "sslmode": os.getenv("PG_SSLMODE", "require"),
}

SOURCE_TABLE = "claims"
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "5000"))
QUARANTINE_DIR = os.getenv("QUARANTINE_DIR", "./quarantine")
PROCESS_NAME = "mysql_to_aurora_claims"

EXPECTED_SOURCE_COLUMNS = {
    "claim_id",
    "patient_id",
    "provider_id",
    "diagnosis_code",
    "procedure_code",
    "amount",
    "service_date",
}

VALIDATION_SQL = """
WITH duplicate_claims AS (
    SELECT claim_id
    FROM claims
    GROUP BY claim_id
    HAVING COUNT(*) > 1
)
SELECT
    c.claim_id,
    c.patient_id,
    c.provider_id,
    c.diagnosis_code,
    c.procedure_code,
    c.amount,
    c.service_date,
    CASE
        WHEN c.claim_id IS NULL THEN 'missing claim_id'
        WHEN c.patient_id IS NULL THEN 'missing patient_id'
        WHEN c.provider_id IS NULL THEN 'missing provider_id'
        WHEN c.procedure_code IS NULL THEN 'missing procedure_code'
        WHEN c.amount IS NULL THEN 'missing amount'
        WHEN c.amount < 0 THEN 'negative amount'
        WHEN c.service_date IS NULL THEN 'missing service_date'
        WHEN c.service_date > CURRENT_DATE() THEN 'future service_date'
        WHEN d.claim_id IS NOT NULL THEN 'duplicate claim_id in source'
        ELSE NULL
    END AS rejection_reason
FROM claims c
LEFT JOIN duplicate_claims d
    ON c.claim_id = d.claim_id
"""

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# =========================================================
# CONNECTIONS
# =========================================================

def get_mysql_connection():
    return mysql.connector.connect(**MYSQL_CONFIG)


def get_pg_connection():
    return psycopg2.connect(**PG_CONFIG)


# =========================================================
# HELPERS
# =========================================================

def generate_load_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") + "_" + uuid.uuid4().hex[:8]


def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def get_mysql_source_columns(mysql_conn, table_name: str) -> set:
    sql = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
    """
    db_name = MYSQL_CONFIG["database"]

    with mysql_conn.cursor() as cur:
        cur.execute(sql, (db_name, table_name))
        rows = cur.fetchall()

    return {row[0].lower() for row in rows}


def validate_source_schema(mysql_conn) -> None:
    actual = get_mysql_source_columns(mysql_conn, SOURCE_TABLE)
    missing = EXPECTED_SOURCE_COLUMNS - actual

    if missing:
        raise RuntimeError(f"Schema drift detected. Missing columns: {sorted(missing)}")

    logger.info("Source schema validation passed.")


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [c.lower() for c in out.columns]
    return out


def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    text_cols = ["claim_id", "patient_id", "provider_id", "diagnosis_code", "procedure_code", "rejection_reason"]
    for col in text_cols:
        if col in out.columns:
            out[col] = out[col].astype("string").str.strip()

    if "amount" in out.columns:
        out["amount"] = pd.to_numeric(out["amount"], errors="coerce")

    if "service_date" in out.columns:
        out["service_date"] = pd.to_datetime(out["service_date"], errors="coerce").dt.date

    return out


def add_metadata(df: pd.DataFrame, load_id: str) -> pd.DataFrame:
    out = df.copy()
    out["load_id"] = load_id
    out["ingested_at"] = datetime.now(timezone.utc)
    return out


def build_record_hash(row: pd.Series) -> str:
    values = [
        str(row.get("claim_id", "")),
        str(row.get("patient_id", "")),
        str(row.get("provider_id", "")),
        str(row.get("diagnosis_code", "")),
        str(row.get("procedure_code", "")),
        str(row.get("amount", "")),
        str(row.get("service_date", "")),
    ]
    payload = "|".join(values)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def add_record_hash(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["record_hash"] = out.apply(build_record_hash, axis=1)
    return out


def split_good_bad(df: pd.DataFrame):
    good_df = df[df["rejection_reason"].isna()].copy()
    bad_df = df[df["rejection_reason"].notna()].copy()
    return good_df, bad_df


def write_bad_records_to_csv(bad_df: pd.DataFrame, load_id: str) -> str:
    ensure_dir(QUARANTINE_DIR)
    file_path = os.path.join(QUARANTINE_DIR, f"claims_bad_records_{load_id}.csv")
    bad_df.to_csv(file_path, index=False, quoting=csv.QUOTE_MINIMAL)
    return file_path


# =========================================================
# POSTGRES LOAD / AUDIT
# =========================================================

def insert_audit_start(pg_conn, load_id: str) -> None:
    sql = """
        INSERT INTO etl.load_audit (
            load_id,
            process_name,
            status,
            started_at
        )
        VALUES (%s, %s, %s, current_timestamp)
    """
    with pg_conn.cursor() as cur:
        cur.execute(sql, (load_id, PROCESS_NAME, "RUNNING"))
    pg_conn.commit()


def update_audit_source_stats(
    pg_conn,
    load_id: str,
    source_rows_read: int,
    source_rows_rejected: int,
    staged_rows_loaded: int
) -> None:
    sql = """
        UPDATE etl.load_audit
        SET
            source_rows_read = %s,
            source_rows_rejected = %s,
            staged_rows_loaded = %s
        WHERE load_id = %s
    """
    with pg_conn.cursor() as cur:
        cur.execute(sql, (source_rows_read, source_rows_rejected, staged_rows_loaded, load_id))
    pg_conn.commit()


def update_audit_failure(pg_conn, load_id: str, error_message: str) -> None:
    sql = """
        UPDATE etl.load_audit
        SET
            status = 'FAILED',
            error_message = %s,
            completed_at = current_timestamp
        WHERE load_id = %s
    """
    with pg_conn.cursor() as cur:
        cur.execute(sql, (error_message[:5000], load_id))
    pg_conn.commit()


def stage_good_rows(pg_conn, good_df: pd.DataFrame) -> int:
    if good_df.empty:
        return 0

    insert_sql = """
        INSERT INTO etl.stg_claims (
            load_id,
            claim_id,
            patient_id,
            provider_id,
            diagnosis_code,
            procedure_code,
            amount,
            service_date,
            ingested_at,
            record_hash
        )
        VALUES %s
    """

    rows = [
        (
            row["load_id"],
            row["claim_id"],
            row["patient_id"],
            row["provider_id"],
            row["diagnosis_code"],
            row["procedure_code"],
            None if pd.isna(row["amount"]) else Decimal(str(row["amount"])),
            row["service_date"],
            row["ingested_at"],
            row["record_hash"],
        )
        for _, row in good_df.iterrows()
    ]

    with pg_conn.cursor() as cur:
        execute_values(cur, insert_sql, rows, page_size=1000)
    pg_conn.commit()

    return len(rows)


def call_merge_procedure(pg_conn, load_id: str) -> None:
    with pg_conn.cursor() as cur:
        cur.execute("CALL etl.sp_merge_claims(%s)", (load_id,))
    pg_conn.commit()


# =========================================================
# MAIN PIPELINE
# =========================================================

def process_claims():
    load_id = generate_load_id()
    logger.info("Starting process for load_id=%s", load_id)

    mysql_conn = None
    pg_conn = None

    total_read = 0
    total_source_rejected = 0
    total_staged = 0
    quarantine_file = None

    all_bad_chunks = []

    try:
        mysql_conn = get_mysql_connection()
        pg_conn = get_pg_connection()

        insert_audit_start(pg_conn, load_id)

        # 1. Validate source schema before extraction
        validate_source_schema(mysql_conn)

        # 2. Extract validated rows from MySQL in chunks
        for chunk_df in pd.read_sql(VALIDATION_SQL, mysql_conn, chunksize=CHUNK_SIZE):
            chunk_df = normalize_dataframe(chunk_df)
            chunk_df = coerce_types(chunk_df)
            chunk_df = add_metadata(chunk_df, load_id)
            chunk_df = add_record_hash(chunk_df)

            total_read += len(chunk_df)

            good_df, bad_df = split_good_bad(chunk_df)

            if not bad_df.empty:
                total_source_rejected += len(bad_df)
                all_bad_chunks.append(bad_df)

            if not good_df.empty:
                staged_count = stage_good_rows(pg_conn, good_df)
                total_staged += staged_count

            logger.info(
                "Chunk processed | rows=%s | good=%s | bad=%s",
                len(chunk_df), len(good_df), len(bad_df)
            )

        # 3. Write source-invalid rows to CSV
        if all_bad_chunks:
            full_bad_df = pd.concat(all_bad_chunks, ignore_index=True)
            quarantine_file = write_bad_records_to_csv(full_bad_df, load_id)
            logger.warning("Source invalid rows written to %s", quarantine_file)

        # 4. Update source-side audit stats
        update_audit_source_stats(
            pg_conn,
            load_id,
            source_rows_read=total_read,
            source_rows_rejected=total_source_rejected,
            staged_rows_loaded=total_staged
        )

        # 5. Call Aurora merge procedure
        call_merge_procedure(pg_conn, load_id)

        result = {
            "status": "SUCCESS",
            "load_id": load_id,
            "source_rows_read": total_read,
            "source_rows_rejected": total_source_rejected,
            "staged_rows_loaded": total_staged,
            "quarantine_file": quarantine_file,
        }

        logger.info("Pipeline finished successfully: %s", result)
        return result

    except Exception as exc:
        logger.exception("Pipeline failed: %s", str(exc))

        if pg_conn:
            try:
                update_audit_failure(pg_conn, load_id, str(exc))
            except Exception:
                logger.exception("Failed to update audit failure state")

        return {
            "status": "FAILED",
            "load_id": load_id,
            "source_rows_read": total_read,
            "source_rows_rejected": total_source_rejected,
            "staged_rows_loaded": total_staged,
            "quarantine_file": quarantine_file,
            "error": str(exc),
        }

    finally:
        if mysql_conn:
            mysql_conn.close()
        if pg_conn:
            pg_conn.close()


if __name__ == "__main__":
    result = process_claims()
    print(result)