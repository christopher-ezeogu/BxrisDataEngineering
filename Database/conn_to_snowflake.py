import snowflake.connector
from typing import List, Dict, Any

def load_to_snowflake(records: List[Dict[str, Any]]) -> None:
    conn = snowflake.connector.connect(
        user="YOUR_USER",
        password="YOUR_PASSWORD",
        account="YOUR_ACCOUNT",
        warehouse="YOUR_WAREHOUSE",
        database="YOUR_DATABASE",
        schema="YOUR_SCHEMA"
    )

    insert_sql = """
        INSERT INTO patient_migration (
            patient_id,
            first_name,
            last_name,
            dob,
            source_system
        )
        VALUES (%(patient_id)s, %(first_name)s, %(last_name)s, %(dob)s, %(source_system)s)
    """

    try:
        cur = conn.cursor()
        for record in records:
            cur.execute(insert_sql, record)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()