"""
1. define required field to load & standardize data types
2. validate for schema drift 
3. get total records received 
4. validate and capture invalid records
5. validate and capture valid records
6. connect to target DB
7. load valid records to DB table(claims)
8. load invalid clains 


"""
import os
import pandas as pd
import psycopg2
import datetime as dt
from io import StringIO

processedDt = dt.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')

REQUIRED_FIELDS = ["claim_id", "patient_id", "provider_id", "diagnosis_code",  "procedure_code",  "claim_amount", "service_date"]

def load_claims(filename):
    try:
    # standardize data types    
        data_types = {
            "claim_id": "Int64",
            "patient_id": "Int64",
            "provider_id": "Int64",
            "diagnosis_code": "string",
            "procedure_code": "Int64",
            "claim_amount": "string",
            "service_date": "string"
        }

    # fetch claim records from CSV
        df = pd.read_csv(filename, dtype=data_types)
        #verify table structure / data
        print(df.head(4))

    # validate for schema drift
        # Normalize column names (prevents hidden schema drift)
        df.columns = df.columns.str.strip().str.lower()
        actual_columns = df.columns.tolist()
        print(actual_columns)

    # Schema drift validation -- Ideally will Raise Notice and still process necessary fields 
        if actual_columns != REQUIRED_FIELDS:
            raise Exception(
                f"Schema drift detected.\n"
                f"Expected columns/order: {REQUIRED_FIELDS}\n"
                f"Actual columns/order: {actual_columns}"
                )

    # get total record count retrieved
        data_row_count = len(df)
        print(f'{data_row_count} records retrieved from CSV')

    # capture invalid record - only rows where fields dont have missing records
        invalid = df[df[REQUIRED_FIELDS].isnull().any(axis=1)]
        #print(invalid.to_dict("records"))
        print(f'{len(invalid)} invalid row(s) retrieved')

    # capture valid records
        valid = df.dropna(subset=REQUIRED_FIELDS)
        #print(valid.to_dict("records"))
        print(f'{len(valid)} valid row(s) retrieved')

    # define storage file path for invalid records
        file_name = '/invalid_records_'+processedDt+'.csv'
        temp_file_name = '/invalid_records_'+processedDt+'.tmp'

        final_file_path = os.getcwd() + file_name
        temp_file_path = os.getcwd() + temp_file_name

    # save dataframe to temp file location.
        invalid.to_csv(temp_file_path, index=False)

    # validate temp archive
        temp_invalid_count = len(pd.read_csv(temp_file_path))
        if temp_invalid_count != len(invalid):
            raise Exception(
                f"Archive validation failed. Expected {len(invalid)} invalid rows, "
                f"but temp archive contains {temp_invalid_count}"
            )

    # connect to target database 
        conn = psycopg2.connect(host='localhost', port=5432, database='lucentis', user='appuser', password='testPwd@1')
        cur = conn.cursor()

    # load valid records
        output = StringIO()
        output.write(valid.to_csv(index=False))
        output.seek(0)

    # copy, insert and validate valid records inserted
        cur.copy_expert(f"COPY etl.stg_claims (claim_id,  patient_id,  provider_id, diagnosis_code,  procedure_code,  amount, service_date) FROM STDIN WITH CSV HEADER", output)
        copied_count = cur.rowcount
        #print(f'{copied_count} valid records CSV inserted successfully')

    # promote temp archive to final archive only after DB load succeeds
        os.replace(temp_file_path, final_file_path)

        archived_invalid_count = len(pd.read_csv(final_file_path))
        if archived_invalid_count != len(invalid):
            raise Exception(
                f"Final archive validation failed. Expected {len(invalid)} invalid rows, "
                f"but archive contains {archived_invalid_count}"
            )

        conn.commit()

        print(f"{copied_count} valid records loaded successfully")
        print(f"{archived_invalid_count} invalid records archived successfully")
        print("DB load and archive completed together")

    except (Exception, psycopg2.DatabaseError)  as error:
        print(f"Error: {error}")

        # rollback DB transaction
        if conn is not None:
            try:
                conn.rollback()
                print("Database transaction rolled back")
            except Exception as rollback_error:
                print(f"Rollback failed: {rollback_error}")

        # cleanup archive files best-effort
        for path in [temp_file_path, final_file_path]:
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


# test function
load_claims("claims.csv")