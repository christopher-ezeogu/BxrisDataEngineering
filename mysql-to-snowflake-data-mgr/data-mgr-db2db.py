import os
import uuid
import json
import logging
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Union

import pandas as pd
import mysql.connector
import psycopg2
from psycopg2.extras import execute_values


# =========================================================
# CONFIG
# =========================================================

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "O*****@1"),
    "database": os.getenv("MYSQL_DATABASE", "lucentis"),
}

PG_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", "O*****1"),
    "dbname": os.getenv("PG_DATABASE", "lucentis_target"),
}

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "5000"))
QUARANTINE_DIR = Path(os.getenv("QUARANTINE_DIR", "./quarantine"))
PROCESS_NAME = "mysql_to_aurora_claims"

SOURCE_SQL = """
SELECT
    c.claim_id,
    c.patient_id,
    c.provider_id,
    c.diagnosis_code,
    c.procedure_code,
    c.amount,
    c.service_date
FROM lucentis.claims_backup c
ORDER BY c.claim_id
"""

VALIDATE_MERGE_CLAIMS_SQL = "SELECT rejected_rows, upserted_rows FROM etl.merge_claims(%s)"

EXPECTED_SOURCE_COLUMNS = [
    "claim_id",
    "patient_id",
    "provider_id",
    "diagnosis_code",
    "procedure_code",
    "amount",
    "service_date",
]

STAGING_COLUMNS = [
    "provider_id",
    "amount",
    "claim_id",
    "service_date",
    "patient_id",
    "procedure_code",
    "diagnosis_code",
    "load_id",
]


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


def ensure_quarantine_dir() -> None:
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)


def extract_claims_in_chunks(mysql_conn, chunk_size: int):
    """
    Streams source rows in chunks instead of loading all rows into memory.
    """
    mysql_cur = mysql_conn.cursor()
    mysql_cur.execute(SOURCE_SQL)
    columns = [desc[0] for desc in mysql_cur.description]

    while True:
        rows = mysql_cur.fetchmany(chunk_size)
        if not rows:
            break
        yield pd.DataFrame(rows, columns=columns)

    mysql_cur.close()


def validate_source_schema(df: pd.DataFrame) -> None:
    """
    Fail fast if the incoming source shape is wrong.
    """
    actual = list(df.columns)
    missing = [c for c in EXPECTED_SOURCE_COLUMNS if c not in actual]
    extra = [c for c in actual if c not in EXPECTED_SOURCE_COLUMNS]

    if missing or extra:
        raise ValueError(
            f"Schema validation failed. Missing columns={missing}, Extra columns={extra}, Actual={actual}"
        )


def normalize_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize types before business validation and COPY.
    """
    work = df.copy()

    work["claim_id"] = pd.to_numeric(work["claim_id"], errors="coerce").astype("Int64")
    work["patient_id"] = pd.to_numeric(work["patient_id"], errors="coerce").astype("Int64")
    work["provider_id"] = pd.to_numeric(work["provider_id"], errors="coerce").astype("Int64")
    work["amount"] = pd.to_numeric(work["amount"], errors="coerce")
    work["service_date"] = pd.to_datetime(work["service_date"], errors="coerce").dt.date

    for col in ["diagnosis_code", "procedure_code"]:
        work[col] = work[col].astype("string")

    return work


def split_valid_invalid_rows(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Apply source-side validation and quarantine invalid rows before COPY.
    """
    today = datetime.now(timezone.utc).date()
    work = df.copy()

    reasons: List[List[str]] = []

    for _, row in work.iterrows():
        row_reasons = []

        if pd.isna(row["claim_id"]):
            row_reasons.append("missing claim_id")
        if pd.isna(row["patient_id"]):
            row_reasons.append("missing patient_id")
        if pd.isna(row["provider_id"]):
            row_reasons.append("missing provider_id")
        if pd.isna(row["procedure_code"]) or str(row["procedure_code"]).strip() == "":
            row_reasons.append("missing procedure_code")
        if pd.isna(row["amount"]):
            row_reasons.append("missing amount")
        elif row["amount"] < 0:
            row_reasons.append("negative amount")
        if pd.isna(row["service_date"]):
            row_reasons.append("missing/invalid service_date")
        elif row["service_date"] > today:
            row_reasons.append("future service_date")

        reasons.append(row_reasons)

    work["rejection_reasons"] = reasons
    work["rejection_reason"] = work["rejection_reasons"].apply(
        lambda x: "; ".join(x) if x else None
    )

    invalid_df = work[work["rejection_reason"].notna()].copy()
    valid_df = work[work["rejection_reason"].isna()].copy()

    # Detect duplicate claim_id within the current chunk as source-side quarantine
    dup_mask = valid_df["claim_id"].duplicated(keep="first")
    duplicate_df = valid_df[dup_mask].copy()
    if not duplicate_df.empty:
        duplicate_df["rejection_reason"] = "duplicate claim_id in source chunk"
        invalid_df = pd.concat([invalid_df, duplicate_df], ignore_index=True)
        valid_df = valid_df[~dup_mask].copy()

    valid_df = valid_df.drop(columns=["rejection_reasons", "rejection_reason"], errors="ignore")
    return valid_df, invalid_df


def prepare_staging_dataframe(valid_df: pd.DataFrame, load_id: str) -> pd.DataFrame:
    staging_df = valid_df.copy()
    staging_df["load_id"] = load_id
    staging_df = staging_df[STAGING_COLUMNS]
    return staging_df


def copy_to_staging(pg_cur, staging_df: pd.DataFrame) -> int:
    if staging_df.empty:
        return 0

    output = StringIO()
    staging_df.to_csv(output, index=False)
    output.seek(0)

    copy_sql = """
        COPY etl.stg_claims
        (provider_id, amount, claim_id, service_date, patient_id, procedure_code, diagnosis_code, load_id)
        FROM STDIN WITH CSV HEADER
    """
    pg_cur.copy_expert(copy_sql, output)
    return len(staging_df)


def write_quarantine_file(load_id: str, chunk_number: int, invalid_df: pd.DataFrame) -> Optional[str]:
    if invalid_df is None or invalid_df.empty:
        return None

    ensure_quarantine_dir()
    file_path = QUARANTINE_DIR / f"{load_id}_chunk_{chunk_number:05d}_bad_rows.csv"
    invalid_df.to_csv(file_path, index=False)
    return str(file_path)


def insert_quarantine_rows(pg_cur, load_id: str, invalid_df: pd.DataFrame) -> int:
    """
    Optional but recommended: persist quarantined rows in Aurora for audit/searchability.
    Requires etl.source_quarantine to exist.
    """
    if invalid_df.empty:
        return 0

    records = []
    for _, row in invalid_df.iterrows():
        records.append((
            load_id,
            safe_int(row.get("claim_id")),
            safe_int(row.get("patient_id")),
            safe_int(row.get("provider_id")),
            safe_str(row.get("diagnosis_code")),
            safe_str(row.get("procedure_code")),
            safe_float(row.get("amount")),
            safe_date_str(row.get("service_date")),
            safe_str(row.get("rejection_reason")),
            json.dumps(row.astype(object).where(pd.notnull(row), None).to_dict(), default=str),
            datetime.now(timezone.utc),
        ))

    sql = """
        INSERT INTO etl.source_quarantine (
            load_id,
            claim_id,
            patient_id,
            provider_id,
            diagnosis_code,
            procedure_code,
            amount,
            service_date,
            rejection_reason,
            source_row_payload,
            quarantined_at
        )
        VALUES %s
    """
    execute_values(pg_cur, sql, records)
    return len(records)


def safe_int(value):
    if pd.isna(value):
        return None
    return int(value)


def safe_float(value):
    if pd.isna(value):
        return None
    return float(value)


def safe_str(value):
    if pd.isna(value):
        return None
    return str(value)


def safe_date_str(value):
    if pd.isna(value):
        return None
    return str(value)


# =========================================================
# AUDIT HELPERS
# =========================================================

def init_audit_row(pg_cur, load_id: str) -> None:
    pg_cur.execute(
        """
        INSERT INTO etl.load_audit (
            load_id,
            process_name,
            status,
            started_at,
            source_rows_read,
            source_rows_quarantined,
            staged_rows_loaded,
            target_rows_rejected,
            target_rows_upserted,
            reconciliation_status
        )
        VALUES (%s, %s, 'STARTED', current_timestamp, 0, 0, 0, 0, 0, 'PENDING')
        """,
        (load_id, PROCESS_NAME)
    )


def update_audit_progress(
    pg_cur,
    load_id: str,
    source_rows_read_delta: int = 0,
    source_rows_quarantined_delta: int = 0,
    staged_rows_loaded_delta: int = 0,
) -> None:
    pg_cur.execute(
        """
        UPDATE etl.load_audit
        SET
            source_rows_read = COALESCE(source_rows_read, 0) + %s,
            source_rows_quarantined = COALESCE(source_rows_quarantined, 0) + %s,
            staged_rows_loaded = COALESCE(staged_rows_loaded, 0) + %s,
            updated_at = current_timestamp
        WHERE load_id = %s
        """,
        (
            source_rows_read_delta,
            source_rows_quarantined_delta,
            staged_rows_loaded_delta,
            load_id,
        )
    )


def finalize_reconciliation(
    pg_cur,
    load_id: str,
    source_rows_read: int,
    source_rows_quarantined: int,
    staged_rows_loaded: int,
    target_rows_rejected: int,
    target_rows_upserted: int,
) -> Dict[str, Union[int , str]]:
    expected_staged = source_rows_read - source_rows_quarantined
    reconciliation_ok = (expected_staged == staged_rows_loaded)

    reconciliation_status = "MATCH" if reconciliation_ok else "MISMATCH"

    pg_cur.execute(
        """
        UPDATE etl.load_audit
        SET
            reconciliation_status = %s,
            completed_at = current_timestamp,
            updated_at = current_timestamp
        WHERE load_id = %s
        """,
        (reconciliation_status, load_id)
    )

    return {
        "expected_staged": expected_staged,
        "actual_staged": staged_rows_loaded,
        "reconciliation_status": reconciliation_status,
        "target_rows_rejected": target_rows_rejected,
        "target_rows_upserted": target_rows_upserted,
    }


def mark_audit_failed(pg_cur, load_id: str, error_message: str) -> None:
    pg_cur.execute(
        """
        UPDATE etl.load_audit
        SET
            status = 'FAILED',
            error_message = %s,
            completed_at = current_timestamp,
            updated_at = current_timestamp
        WHERE load_id = %s
        """,
        (error_message[:4000], load_id)
    )


# =========================================================
# MERGE RESPONSE
# =========================================================

def parse_merge_response(row) -> Dict[str, int]:
    """
    Your PostgreSQL function returns:
    RETURNS TABLE(rejected_rows integer, upserted_rows integer)

    So row[0] = rejected_rows
       row[1] = upserted_rows
    """
    if row is None:
        return {"rejected_rows": 0, "upserted_rows": 0}

    if isinstance(row, tuple):
        if len(row) >= 2:
            return {
                "rejected_rows": int(row[0] or 0),
                "upserted_rows": int(row[1] or 0),
            }
        if len(row) == 1:
            return {
                "rejected_rows": int(row[0] or 0),
                "upserted_rows": 0,
            }

    if isinstance(row, dict):
        return {
            "rejected_rows": int(row.get("rejected_rows", 0)),
            "upserted_rows": int(row.get("upserted_rows", 0)),
        }

    raise ValueError(f"Unexpected merge_claims response format: {row}")


# =========================================================
# MAIN PIPELINE
# =========================================================

def process_claims():
    load_id = generate_load_id()
    logger.info("Starting process for load_id=%s", load_id)

    mysql_conn = None
    pg_conn = None
    pg_cur = None

    total_source_rows_read = 0
    total_source_rows_quarantined = 0
    total_staged_rows_loaded = 0
    quarantine_files: List[str] = []

    try:
        mysql_conn = get_mysql_connection()
        pg_conn = get_pg_connection()
        pg_conn.autocommit = False
        pg_cur = pg_conn.cursor()

        init_audit_row(pg_cur, load_id)
        pg_conn.commit()

        chunk_number = 0

        for raw_df in extract_claims_in_chunks(mysql_conn, CHUNK_SIZE):
            chunk_number += 1
            logger.info("Processing chunk=%s with %s rows", chunk_number, len(raw_df))

            validate_source_schema(raw_df)
            normalized_df = normalize_types(raw_df)

            source_rows_read = len(normalized_df)
            total_source_rows_read += source_rows_read

            valid_df, invalid_df = split_valid_invalid_rows(normalized_df)

            source_rows_quarantined = len(invalid_df)
            staged_candidate_rows = len(valid_df)

            total_source_rows_quarantined += source_rows_quarantined

            quarantine_file = write_quarantine_file(load_id, chunk_number, invalid_df)
            if quarantine_file:
                quarantine_files.append(quarantine_file)

            if not invalid_df.empty:
                insert_quarantine_rows(pg_cur, load_id, invalid_df)

            staging_df = prepare_staging_dataframe(valid_df, load_id)
            staged_rows_loaded = copy_to_staging(pg_cur, staging_df)
            total_staged_rows_loaded += staged_rows_loaded

            update_audit_progress(
                pg_cur,
                load_id,
                source_rows_read_delta=source_rows_read,
                source_rows_quarantined_delta=source_rows_quarantined,
                staged_rows_loaded_delta=staged_rows_loaded,
            )

            logger.info(
                "Chunk=%s complete | source=%s | quarantined=%s | staged=%s",
                chunk_number,
                source_rows_read,
                source_rows_quarantined,
                staged_rows_loaded,
            )

            if staged_candidate_rows != staged_rows_loaded:
                raise ValueError(
                    f"Chunk reconciliation failed for chunk {chunk_number}: "
                    f"valid_rows={staged_candidate_rows}, staged_rows_loaded={staged_rows_loaded}"
                )

            pg_conn.commit()

        logger.info(
            "Extraction complete | total_source=%s | total_quarantined=%s | total_staged=%s",
            total_source_rows_read,
            total_source_rows_quarantined,
            total_staged_rows_loaded,
        )

        pg_cur.execute(VALIDATE_MERGE_CLAIMS_SQL, (load_id,))
        merge_row = pg_cur.fetchone()
        merge_result = parse_merge_response(merge_row)

        reconciliation = finalize_reconciliation(
            pg_cur,
            load_id,
            source_rows_read=total_source_rows_read,
            source_rows_quarantined=total_source_rows_quarantined,
            staged_rows_loaded=total_staged_rows_loaded,
            target_rows_rejected=merge_result["rejected_rows"],
            target_rows_upserted=merge_result["upserted_rows"],
        )

        pg_conn.commit()

        result = {
            "status": "SUCCESS",
            "load_id": load_id,
            "source_rows_read": total_source_rows_read,
            "source_rows_quarantined": total_source_rows_quarantined,
            "staged_rows_loaded": total_staged_rows_loaded,
            "target_rows_rejected": merge_result["rejected_rows"],
            "target_rows_upserted": merge_result["upserted_rows"],
            "expected_staged": reconciliation["expected_staged"],
            "actual_staged": reconciliation["actual_staged"],
            "reconciliation_status": reconciliation["reconciliation_status"],
            "quarantine_files": quarantine_files,
        }

        logger.info("Pipeline finished successfully: %s", result)
        return result

    except Exception as exc:
        logger.exception("Pipeline failed for load_id=%s: %s", load_id, str(exc))

        if pg_conn:
            try:
                pg_conn.rollback()
                if pg_cur:
                    mark_audit_failed(pg_cur, load_id, str(exc))
                    pg_conn.commit()
            except Exception:
                logger.exception("Failed to update audit row to FAILED for load_id=%s", load_id)

        return {
            "status": "FAILED",
            "load_id": load_id,
            "error": str(exc),
            "source_rows_read": total_source_rows_read,
            "source_rows_quarantined": total_source_rows_quarantined,
            "staged_rows_loaded": total_staged_rows_loaded,
            "quarantine_files": quarantine_files,
        }

    finally:
        if pg_cur:
            pg_cur.close()
        if mysql_conn:
            mysql_conn.close()
        if pg_conn:
            pg_conn.close()


if __name__ == "__main__":
    result = process_claims()
    print(json.dumps(result, indent=2, default=str))