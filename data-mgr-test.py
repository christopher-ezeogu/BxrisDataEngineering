"""
step 1 - safely read csv file
step 2 - check if there is any truncated field
step 3 - check if there is extra column by (get the fields and check with expected fields)
     # Skip empty lines
     # Detect broken quoted rows
     # Parse row safely
     # Detect too few / extra columns
     # Row has extra columns
step 4 - retrieve clean records 
    - convert it to dataframe - reorder to match expected_fields
step 5 - standardize data types
    - load clean data
 
step 6 - Apply business/data quality checks
        # missing required fields
        # duplicate claim_id
        # negative claim_amount
        # invalid procedure_code
        # ensure patient_id is integer only
        # ensure provider is integer only 
        # malformed service_date
        # future service_date

step 7. Split clean vs bad business rows
    - retrieve business_bad_df & clean_df in a variable
    - add load_id to track load
    - get count of both clean and bad records

step 8. load records to database and archive bad records
 - define storage file path for invalid records
 - save dataframe to temp location as .tmp to ensure that failures can be rollbacked without commiting
 - connect to database and load converted clean_df to csv
 -


Production Grade:
-------------------
reads the file in chunks
validates structural issues per chunk
converts each chunk to a DataFrame
applies business checks per chunk
loads only the clean chunk to PostgreSQL immediately
appends bad business rows and structural bad rows to archive files incrementally
keeps the DB transaction open until the end, so a failure rolls back the whole load
only promotes temp archive files to final files after everything succeeds

"""


import csv
import uuid
import datetime as dt
import pandas as pd
import os
import psycopg2
from io import StringIO


# =========================================================
# CONFIG
# =========================================================

expected_fields = [
    "provider_id",
    "claim_amount",
    "claim_id",
    "service_date",
    "patient_id",
    "procedure_code",
    "diagnosis_code"
]

valid_cpt_codes = [99213, 80050, 93000, 71020, 99214, 36415]
CHUNK_SIZE = 10000


# =========================================================
# HELPERS
# =========================================================

def is_integer_string(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().str.fullmatch(r"\d+")


def append_df_to_csv(df: pd.DataFrame, file_path: str) -> int:
    if df.empty:
        return 0

    file_exists = os.path.exists(file_path)
    write_header = (not file_exists) or os.path.getsize(file_path) == 0

    df.to_csv(file_path, mode="a", header=write_header, index=False)
    return len(df)


def append_dict_rows_to_csv(rows: list, file_path: str, fieldnames: list) -> int:
    if not rows:
        return 0

    file_exists = os.path.exists(file_path)
    write_header = (not file_exists) or os.path.getsize(file_path) == 0

    with open(file_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)

    return len(rows)


def process_structural_chunk(chunk_rows, header):
    structural_bad_rows = []
    structurally_clean_rows = []

    for line_number, raw_line in chunk_rows:
        raw_line_stripped = raw_line.rstrip("\n")

        # Skip empty lines
        if not raw_line_stripped.strip():
            structural_bad_rows.append({
                "line_number": line_number,
                "reason": "empty row",
                "raw_row": raw_line_stripped
            })
            continue

        # Detect broken quoted rows
        if raw_line.count('"') % 2 != 0:
            structural_bad_rows.append({
                "line_number": line_number,
                "reason": "broken quoted row",
                "raw_row": raw_line_stripped
            })
            continue

        # Parse row safely
        try:
            parsed = next(csv.reader([raw_line]))
        except Exception:
            structural_bad_rows.append({
                "line_number": line_number,
                "reason": "unparseable row",
                "raw_row": raw_line_stripped
            })
            continue

        # Detect too few / extra columns
        if len(parsed) < len(header):
            structural_bad_rows.append({
                "line_number": line_number,
                "reason": "row has too few columns",
                "raw_row": raw_line_stripped
            })
            continue

        if len(parsed) > len(header):
            structural_bad_rows.append({
                "line_number": line_number,
                "reason": "row has extra columns",
                "raw_row": raw_line_stripped
            })
            continue

        structurally_clean_rows.append(parsed)

    return structurally_clean_rows, structural_bad_rows


def apply_business_checks(df: pd.DataFrame, load_id: str):
    if df.empty:
        empty_df = pd.DataFrame(columns=df.columns.tolist() + ["load_id"])
        return empty_df.copy(), empty_df.copy()

    # Reorder columns to expected order
    df = df[expected_fields].copy()

    # Standardize string columns first
    for col in ["claim_id", "patient_id", "provider_id", "procedure_code", "diagnosis_code"]:
        df[col] = df[col].astype("string").str.strip()

    # Convert only fields that should truly be numeric/date at this stage
    df["claim_amount"] = pd.to_numeric(df["claim_amount"], errors="coerce")
    df["service_date"] = pd.to_datetime(df["service_date"], errors="coerce")
    df["diagnosis_code"] = df["diagnosis_code"].astype("string")

    bad_index = set()

    # Integer-only validation first
    valid_claim_id_mask = is_integer_string(df["claim_id"])
    valid_patient_id_mask = is_integer_string(df["patient_id"])
    valid_provider_id_mask = is_integer_string(df["provider_id"])
    valid_procedure_code_mask = is_integer_string(df["procedure_code"])

    bad_index.update(df[~valid_claim_id_mask].index)
    bad_index.update(df[~valid_patient_id_mask].index)
    bad_index.update(df[~valid_provider_id_mask].index)
    bad_index.update(df[~valid_procedure_code_mask].index)

    # Convert validated integer fields
    if valid_claim_id_mask.any():
        df["claim_id"] = df["claim_id"].where(valid_claim_id_mask).astype("Int64")
        df["patient_id"] = df["patient_id"].where(valid_patient_id_mask).astype("Int64")
        df["provider_id"] = df["provider_id"].where(valid_provider_id_mask).astype("Int64")
        df["procedure_code"] = df["procedure_code"].where(valid_procedure_code_mask).astype("Int64")

    # Missing required fields
    missing_required = df[df[expected_fields].isnull().any(axis=1)]
    bad_index.update(missing_required.index)

    # Duplicate claim_id inside chunk
    duplicate_claims = df[df.duplicated("claim_id", keep=False)]
    bad_index.update(duplicate_claims.index)

    # Negative claim_amount
    negative_claim_amount = df[df["claim_amount"] < 0]
    bad_index.update(negative_claim_amount.index)

    # Invalid procedure_code against allowed CPT list
    invalid_procedure_code = df[~df["procedure_code"].isin(valid_cpt_codes)]
    bad_index.update(invalid_procedure_code.index)

    # Malformed service_date
    malformed_service_date = df[df["service_date"].isna()]
    bad_index.update(malformed_service_date.index)

    # Future service_date
    today = pd.Timestamp.today().normalize()
    future_service_date = df[df["service_date"] > today]
    bad_index.update(future_service_date.index)

    business_bad_df = df.loc[sorted(bad_index)].copy()
    clean_df = df.drop(index=bad_index).copy()

    business_bad_df["load_id"] = load_id
    clean_df["load_id"] = load_id

    if not clean_df.empty:
        clean_df = clean_df.astype({
            "claim_id": "Int64",
            "patient_id": "Int64",
            "provider_id": "Int64",
            "procedure_code": "Int64",
            "diagnosis_code": "string"
        })

    return clean_df, business_bad_df


def load_clean_chunk_to_db(cur, clean_df: pd.DataFrame):
    if clean_df.empty:
        return 0

    output = StringIO()
    output.write(clean_df.to_csv(index=False))
    output.seek(0)

    cur.copy_expert(
        """
        COPY etl.stg_claims
        (provider_id, amount, claim_id, service_date, patient_id, procedure_code, diagnosis_code, load_id)
        FROM STDIN WITH CSV HEADER
        """,
        output
    )

    return len(clean_df)


# =========================================================
# MAIN FUNCTION
# =========================================================

def load_claims_v3(file_name, chunk_size=CHUNK_SIZE):
    conn = None
    cur = None
    temp_invalid_file_path = None
    final_invalid_file_path = None
    temp_structural_file_path = None
    final_structural_file_path = None

    processed_dt = dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    load_id = dt.datetime.now().strftime("%Y%m%d%H%M%S") + "_" + uuid.uuid4().hex[:8]

    total_structural_bad = 0
    total_structural_clean = 0
    total_business_bad = 0
    total_loaded = 0
    total_rows_read = 0

    structural_fieldnames = ["line_number", "reason", "raw_row"]

    try:
        invalid_file_name = f"invalid_records_{processed_dt}.tmp"
        structural_file_name = f"structural_bad_records_{processed_dt}.tmp"

        final_invalid_name = f"invalid_records_{processed_dt}.csv"
        final_structural_name = f"structural_bad_records_{processed_dt}.csv"

        temp_invalid_file_path = os.path.join(os.getcwd(), invalid_file_name)
        temp_structural_file_path = os.path.join(os.getcwd(), structural_file_name)

        final_invalid_file_path = os.path.join(os.getcwd(), final_invalid_name)
        final_structural_file_path = os.path.join(os.getcwd(), final_structural_name)

        with open(file_name, "r", newline="", encoding="utf-8") as f:
            try:
                header_line = next(f)
            except StopIteration:
                raise ValueError("Input file is empty")

            # Parse header
            try:
                header = next(csv.reader([header_line]))
            except Exception as e:
                raise ValueError(f"Failed to parse header: {e}")

            header = [col.strip().lower() for col in header]
            print("Detected columns:", header)

            missing_header_cols = [c for c in expected_fields if c not in header]
            extra_header_cols = [c for c in header if c not in expected_fields]

            if missing_header_cols:
                raise ValueError(f"Missing expected columns in header: {missing_header_cols}")

            if extra_header_cols:
                raise ValueError(f"Unexpected extra columns in header: {extra_header_cols}")

            conn = psycopg2.connect(
                host="localhost",
                port=5432,
                database="lucentis",
                user="appuser",
                password="testPwd@1"
            )
            cur = conn.cursor()

            chunk_rows = []
            last_line_number = 1
            last_line_text = header_line

            for line_number, raw_line in enumerate(f, start=2):
                total_rows_read += 1
                chunk_rows.append((line_number, raw_line))
                last_line_number = line_number
                last_line_text = raw_line

                if len(chunk_rows) >= chunk_size:
                    structurally_clean_rows, structural_bad_rows = process_structural_chunk(chunk_rows, header)

                    structural_bad_written = append_dict_rows_to_csv(
                        structural_bad_rows,
                        temp_structural_file_path,
                        structural_fieldnames
                    )
                    total_structural_bad += structural_bad_written
                    total_structural_clean += len(structurally_clean_rows)

                    if structurally_clean_rows:
                        df_chunk = pd.DataFrame(structurally_clean_rows, columns=header)
                        clean_df, business_bad_df = apply_business_checks(df_chunk, load_id)

                        total_business_bad += append_df_to_csv(business_bad_df, temp_invalid_file_path)
                        total_loaded += load_clean_chunk_to_db(cur, clean_df)

                    chunk_rows = []

            # Process final chunk
            if chunk_rows:
                structurally_clean_rows, structural_bad_rows = process_structural_chunk(chunk_rows, header)

                structural_bad_written = append_dict_rows_to_csv(
                    structural_bad_rows,
                    temp_structural_file_path,
                    structural_fieldnames
                )
                total_structural_bad += structural_bad_written
                total_structural_clean += len(structurally_clean_rows)

                if structurally_clean_rows:
                    df_chunk = pd.DataFrame(structurally_clean_rows, columns=header)
                    clean_df, business_bad_df = apply_business_checks(df_chunk, load_id)

                    total_business_bad += append_df_to_csv(business_bad_df, temp_invalid_file_path)
                    total_loaded += load_clean_chunk_to_db(cur, clean_df)

            # Detect truncated final row
            if last_line_number > 1 and not last_line_text.endswith("\n"):
                truncated_row = [{
                    "line_number": last_line_number,
                    "reason": "truncated final row",
                    "raw_row": last_line_text.rstrip("\n")
                }]
                total_structural_bad += append_dict_rows_to_csv(
                    truncated_row,
                    temp_structural_file_path,
                    structural_fieldnames
                )

        # Promote temp archive files only after DB work succeeds
        if os.path.exists(temp_invalid_file_path):
            os.replace(temp_invalid_file_path, final_invalid_file_path)

        if os.path.exists(temp_structural_file_path):
            os.replace(temp_structural_file_path, final_structural_file_path)

        conn.commit()

        print(f"load_id: {load_id}")
        print(f"source rows read: {total_rows_read}")
        print(f"structurally clean rows: {total_structural_clean}")
        print(f"structurally bad rows: {total_structural_bad}")
        print(f"business-invalid rows archived: {total_business_bad}")
        print(f"valid rows loaded: {total_loaded}")
        print("DB load and archive completed together")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")

        if conn is not None:
            try:
                conn.rollback()
                print("Database transaction rolled back")
            except Exception as rollback_error:
                print(f"Rollback failed: {rollback_error}")

        for path in [
            temp_invalid_file_path,
            final_invalid_file_path,
            temp_structural_file_path,
            final_structural_file_path
        ]:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                    print(f"Removed archive file during rollback: {path}")
                except Exception as cleanup_error:
                    print(f"Archive cleanup failed for {path}: {cleanup_error}")

    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass

        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# test
load_claims_v3("claims_harder_dataset_corrupted.csv")