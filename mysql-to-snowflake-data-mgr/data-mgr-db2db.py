import os
import uuid
import logging
from datetime import datetime, timezone
from io import StringIO

import pandas as pd
import mysql.connector
import psycopg2


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

VALIDATE_MERGE_CLAIMS_SQL = "SELECT * FROM etl.merge_claims(%s)"
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "5000"))
QUARANTINE_DIR = os.getenv("QUARANTINE_DIR", "./quarantine")
PROCESS_NAME = "mysql_to_aurora_claims"

RETRIEVE_SQL = """
SELECT
    c.claim_id,
    c.patient_id,
    c.provider_id,
    c.diagnosis_code,
    c.procedure_code,
    c.amount,
    c.service_date
FROM claims c
"""

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


def extract_claims_to_dataframe(mysql_conn) -> pd.DataFrame:
    mysql_cur = mysql_conn.cursor()
    mysql_cur.execute(RETRIEVE_SQL)
    records = mysql_cur.fetchall()
    columns = [desc[0] for desc in mysql_cur.description]
    mysql_cur.close()

    df = pd.DataFrame(records, columns=columns)
    return df


def prepare_staging_dataframe(df: pd.DataFrame, load_id: str) -> pd.DataFrame:
    df = df.copy()

    # Add load_id required by staging table
    df["load_id"] = load_id

    # Reorder columns to match COPY target exactly
    df = df[STAGING_COLUMNS]

    return df


def copy_to_staging(pg_cur, df: pd.DataFrame) -> int:
    output = StringIO()
    df.to_csv(output, index=False)
    output.seek(0)

    copy_sql = """
        COPY etl.stg_claims
        (provider_id, amount, claim_id, service_date, patient_id, procedure_code, diagnosis_code, load_id)
        FROM STDIN WITH CSV HEADER
    """
    pg_cur.copy_expert(copy_sql, output)
    return len(df)


def parse_merge_response(rows):
    """
    Assumes merge_claims returns one row.
    Adjust this if your function returns a different shape.
    """
    if not rows:
        return {"upserted_rows": 0, "rejected_rows": 0}

    row = rows[0]

    # Handle tuple-based response safely
    if isinstance(row, tuple):
        if len(row) >= 2:
            return {
                "upserted_rows": row[0],
                "rejected_rows": row[1],
            }
        elif len(row) == 1:
            return {
                "upserted_rows": row[0],
                "rejected_rows": 0,
            }

    # Fallback if using named cursor or dict-like row
    if isinstance(row, dict):
        return {
            "upserted_rows": row.get("upserted_rows", 0),
            "rejected_rows": row.get("rejected_rows", 0),
        }

    raise ValueError(f"Unexpected merge function response format: {rows}")


# =========================================================
# MAIN PIPELINE
# =========================================================

def process_claims():
    load_id = generate_load_id()
    logger.info("Starting process for load_id=%s", load_id)

    mysql_conn = None
    pg_conn = None

    try:
        mysql_conn = get_mysql_connection()
        pg_conn = get_pg_connection()

        # Start explicit transaction control
        pg_conn.autocommit = False

        # 1. Extract from MySQL
        df = extract_claims_to_dataframe(mysql_conn)
        source_rows_read = len(df)
        logger.info("Extracted %s rows from MySQL", source_rows_read)

        if df.empty:
            result = {
                "status": "SUCCESS",
                "load_id": load_id,
                "source_rows_read": 0,
                "rows_rejected": 0,
                "valid_records": 0,
                "staged_rows_loaded": 0,
                "message": "No records found in source."
            }
            logger.info("Pipeline finished successfully: %s", result)
            return result

        # 2. Prepare data for staging
        staging_df = prepare_staging_dataframe(df, load_id)

        # 3. Load to Aurora staging
        pg_cur = pg_conn.cursor()
        staged_rows_loaded = copy_to_staging(pg_cur, staging_df)
        logger.info("%s records successfully loaded into staging", staged_rows_loaded)

        # 4. Call Aurora merge function
        pg_cur.execute(VALIDATE_MERGE_CLAIMS_SQL, (load_id,))
        merge_rows = pg_cur.fetchall()
        merge_result = parse_merge_response(merge_rows)

        # 5. Commit only after staging + merge both succeed
        pg_conn.commit()

        result = {
            "status": "SUCCESS",
            "load_id": load_id,
            "source_rows_read": source_rows_read,
            "rows_rejected": merge_result["rejected_rows"],
            "valid_records": merge_result["upserted_rows"],
            "staged_rows_loaded": staged_rows_loaded,
        }

        logger.info("Pipeline finished successfully: %s", result)
        pg_cur.close()
        return result

    except Exception as exc:
        if pg_conn:
            pg_conn.rollback()

        logger.exception("Pipeline failed for load_id=%s: %s", load_id, str(exc))
        return {
            "status": "FAILED",
            "load_id": load_id,
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