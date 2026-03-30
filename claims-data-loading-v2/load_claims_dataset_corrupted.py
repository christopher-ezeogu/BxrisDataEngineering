"""
Test Data:
- claims_harder_dataset_corrupted.csv
    tests validation logic, schema normalization, type coercion, dedupe, and business-rule rejection
    tests whether your ingestion can survive malformed raw files without crashing the whole pipeline

Requirements (clean out the following bad data)
    - missing required fields
    - duplicate claim_id
    - negative claim_amount
    - invalid procedure_code
    - mixed data types like patient_id = "UNK"
    - malformed and future service_date
    - columns intentionally out of order
    - rows with too few columns
    - rows with extra columns
    - broken quoted rows
    - a truncated final row

**** Solution ****

Structural bad rows
These are removed before pandas processing:
    - rows with too few columns
    - rows with extra columns
    - broken quoted rows
    - truncated final row
    - empty/unparseable rows

Business bad rows
These are quarantined after parsing:
    - missing required fields
    - duplicate claim_id
    - negative claim_amount
    - invalid procedure_code
    - invalid patient_id like UNK
    - malformed service_date
    - future service_date    


"""
import csv
import os
import uuid
import datetime as dt
import pandas as pd
import psycopg2
from io import StringIO

expected_fields = ["provider_id", "claim_amount", "claim_id", "service_date", "patient_id", "procedure_code", "diagnosis_code"]

valid_cpt_codes = [99213, 80050, 93000, 99214, 71020]

processedDt = dt.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

load_id = dt.datetime.now().strftime("%Y%m%d%H%M%S") + "_" + uuid.uuid4().hex[:8]

expected_fields = [
    "provider_id",
    "claim_amount",
    "claim_id",
    "service_date",
    "patient_id",
    "procedure_code",
    "diagnosis_code"
]

valid_cpt_codes = {99213, 80050, 93000, 71020, 99214, 36415}


def load_claims_v2(file_name):
   try:
        bad_rows = []
        clean_rows = []

        # ----------------------------
        # 1. Read raw file safely
        # ----------------------------
        with open(file_name, "r", newline="", encoding="utf-8") as f:
            lines = f.readlines()

        if not lines:
            raise ValueError("Input file is empty")

        # Detect truncated final row
        if not lines[-1].endswith("\n"):
            bad_rows.append({
                "line_number": len(lines),
                "reason": "truncated final row",
                "raw_row": lines[-1].rstrip("\n")
            })

        # ----------------------------
        # 2. Parse header
        # ----------------------------
        try:
            header = next(csv.reader([lines[0]]))
        except Exception as e:
            raise ValueError(f"Failed to parse header: {e}")

        header = [col.strip().lower() for col in header]

        print("Detected columns:", header)

        # Detect extra / missing columns in header
        missing_header_cols = [c for c in expected_fields if c not in header]
        extra_header_cols = [c for c in header if c not in expected_fields]

        if missing_header_cols:
            raise ValueError(f"Missing expected columns in header: {missing_header_cols}")

        if extra_header_cols:
            raise ValueError(f"Unexpected extra columns in header: {extra_header_cols}")

        # Detect out-of-order columns
        if header != expected_fields:
            print("WARNING: Columns are out of order. Reordering will be applied.")
            print("Expected:", expected_fields)
            print("Actual:  ", header)

        # ----------------------------
        # 3. Validate each raw row
        # ----------------------------
        for line_number, raw_line in enumerate(lines[1:], start=2):
            raw_line_stripped = raw_line.rstrip("\n")

            # Skip empty lines
            if not raw_line_stripped.strip():
                bad_rows.append({
                    "line_number": line_number,
                    "reason": "empty row",
                    "raw_row": raw_line_stripped
                })
                continue

            # Detect broken quoted rows
            if raw_line.count('"') % 2 != 0:
                bad_rows.append({
                    "line_number": line_number,
                    "reason": "broken quoted row",
                    "raw_row": raw_line_stripped
                })
                continue

            # Parse row safely
            try:
                parsed = next(csv.reader([raw_line]))
            except Exception:
                bad_rows.append({
                    "line_number": line_number,
                    "reason": "unparseable row",
                    "raw_row": raw_line_stripped
                })
                continue

            # Detect too few / extra columns
            if len(parsed) < len(header):
                bad_rows.append({
                    "line_number": line_number,
                    "reason": "row has too few columns",
                    "raw_row": raw_line_stripped
                })
                continue

            if len(parsed) > len(header):
                bad_rows.append({
                    "line_number": line_number,
                    "reason": "row has extra columns",
                    "raw_row": raw_line_stripped
                })
                continue

            clean_rows.append(parsed)

        # ----------------------------
        # 4. Load clean raw rows into DataFrame
        # ----------------------------
        df = pd.DataFrame(clean_rows, columns=header)

        # Reorder columns to expected order
        df = df[expected_fields]

        print(f"{len(df)} structurally valid rows loaded")
        print(f"{len(bad_rows)} structurally bad rows quarantined")

        # ----------------------------
        # 5. Standardize types
        # ----------------------------
        df["claim_id"] = pd.to_numeric(df["claim_id"], errors="coerce")
        df["patient_id"] = pd.to_numeric(df["patient_id"], errors="coerce")
        df["provider_id"] = pd.to_numeric(df["provider_id"], errors="coerce")
        df["procedure_code"] = pd.to_numeric(df["procedure_code"], errors="coerce")
        df["claim_amount"] = pd.to_numeric(df["claim_amount"], errors="coerce")
        df["service_date"] = pd.to_datetime(df["service_date"], errors="coerce")
        df["diagnosis_code"] = df["diagnosis_code"].astype("string")

        # ----------------------------
        # 6. Apply business/data quality checks
        # ----------------------------
        bad_index = set()

        # missing required fields
        missing_required = df[df[expected_fields].isnull().any(axis=1)]
        print(f"{len(missing_required)} records with missing required fields")
        bad_index.update(missing_required.index)

        # duplicate claim_id
        duplicate_claims = df[df.duplicated("claim_id", keep=False)]
        print(f"{len(duplicate_claims)} records with duplicate claim_id")
        bad_index.update(duplicate_claims.index)

        # negative claim_amount
        negative_claim_amount = df[df["claim_amount"] < 0]
        print(f"{len(negative_claim_amount)} records with negative claim_amount")
        bad_index.update(negative_claim_amount.index)

        # invalid procedure_code
        invalid_procedure_code = df[~df["procedure_code"].isin(valid_cpt_codes)]
        print(f"{len(invalid_procedure_code)} records with invalid procedure_code")
        bad_index.update(invalid_procedure_code.index)

        # ensure patient_id is integer only & mixed / invalid patient_id like UNK
        invalid_patient_id = df[df["patient_id"].isna() | (df["patient_id"] % 1 != 0)]
        print(f"{len(invalid_patient_id)} records with invalid patient_id")
        bad_index.update(invalid_patient_id.index)

        # ensure provider is integer only 
        invalid_provider_id = df[df["provider_id"].isna() | (df["provider_id"] % 1 !=0)]
        print(f"{len(invalid_provider_id)} records with invalid provider_id")
        bad_index.update(invalid_provider_id.index)

        # malformed service_date
        malformed_service_date = df[df["service_date"].isna()]
        print(f"{len(malformed_service_date)} records with malformed service_date")
        bad_index.update(malformed_service_date.index)

        # future service_date
        today = pd.Timestamp.today().normalize()
        future_service_date = df[df["service_date"] > today]
        print(f"{len(future_service_date)} records with future service_date")
        bad_index.update(future_service_date.index)

        # ----------------------------
        # 7. Split clean vs bad business rows
        # ----------------------------
        business_bad_df = df.loc[sorted(bad_index)].copy()
        clean_df = df.drop(index=bad_index).copy()

        # add load_id field to the batch before loading
        business_bad_df["load_id"] = load_id
        clean_df["load_id"] = load_id

        print(f"{len(clean_df)} fully clean claim records ready for load")
        print(f"{len(business_bad_df)} business-invalid claim records quarantined")

        # Optional: convert final dtypes for clean output
        clean_df = clean_df.astype({
            "claim_id": "Int64",
            "patient_id": "Int64",
            "provider_id": "Int64",
            "procedure_code": "Int64",
            "diagnosis_code": "string"
        })

        #-----------------------------------------------------
        # 8. load records to database and archive bad records
        #-----------------------------------------------------

        # define storage file path for invalid records
        file_name = '/invalid_records_'+processedDt+'.csv'
        temp_file_name = '/invalid_records_'+processedDt+'.tmp'

        structural_bad_records_file_name = '/structural_bad_records_'+processedDt+'.csv'
        temp_structural_bad_records_file_name = '/structural_bad_records_'+processedDt+'.tmp'

        final_file_path = os.getcwd() + file_name
        temp_file_path = os.getcwd() + temp_file_name

        final_structural_bad_record_file_path = os.getcwd() + structural_bad_records_file_name
        temp_structural_bad_record_file_path = os.getcwd() + temp_structural_bad_records_file_name

        # save dataframe to temp file location.
        business_bad_df.to_csv(temp_file_path, index=False)
        pd.DataFrame(bad_rows).to_csv(temp_structural_bad_record_file_path, index=False)

        # validate temp archive
        temp_invalid_count = len(pd.read_csv(temp_file_path))
        if temp_invalid_count != len(business_bad_df):
            raise Exception(
                f"Archive validation failed. Expected {len(business_bad_df)} invalid rows, "
                f"but temp archive contains {temp_invalid_count}"
            )

        # connect to target database 
        conn = psycopg2.connect(host='localhost', port=5432, database='lucentis', user='appuser', password='testPwd@1')
        cur = conn.cursor()

        # load valid records
        output = StringIO()
        output.write(clean_df.to_csv(index=False))
        output.seek(0)

        # copy, insert and validate valid records inserted
        cur.copy_expert(f"COPY etl.stg_claims (provider_id, amount, claim_id, service_date, patient_id, procedure_code, diagnosis_code, load_id) FROM STDIN WITH CSV HEADER", output)
        copied_count = cur.rowcount

        # promote temp archive to final archive only after DB load succeeds
        os.replace(temp_file_path, final_file_path)
        os.replace(temp_structural_bad_record_file_path, final_structural_bad_record_file_path)

        archived_invalid_count = len(pd.read_csv(final_file_path))
        if archived_invalid_count != len(business_bad_df):
            raise Exception(
                f"Final archive validation failed. Expected {len(business_bad_df)} invalid rows, "
                f"but archive contains {archived_invalid_count}"
            )

        conn.commit()

        print(f"{copied_count} valid records loaded successfully")
        print(f"{archived_invalid_count} invalid records archived successfully")
        print("DB load and archive completed together")
        
   except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")

        # rollback DB transaction
        if conn is not None:
            try:
                conn.rollback()
                print("Database transaction rolled back")
            except Exception as rollback_error:
                print(f"Rollback failed: {rollback_error}")

        # cleanup archive files best-effort
        for path in [temp_file_path, final_file_path, temp_structural_bad_record_file_path, final_structural_bad_record_file_path]:
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
load_claims_v2("claims_harder_dataset_corrupted.csv")

