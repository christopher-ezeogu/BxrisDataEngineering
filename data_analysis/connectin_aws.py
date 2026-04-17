import boto3
import json
import io
import os
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, filename="fsv_file_processing.log", format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize AWS Clients
ssm = boto3.client("ssm")
s3_client = boto3.client('s3')

# Get Environment
env = "DEV" #os.environ.get("env")

def archive_failed_records(failed_records, programName, dbname):
    """Save failed records to S3 for later processing"""
    if not failed_records:
        return
    
    #fsv_params = ssm.get_parameters(Names=["/BXRIS/ETL/ADF/fsvParams"], WithDecryption=True)
    #fsv_params_vals = fsv_params['Parameters'][0]['Value']
    #fsv_param_dict = json.loads(fsv_params_vals)

    FSV_S3_BUCKET_NAME = 'bxris-common-dev' #fsv_param_dict['s3_buckets_and_folders']['buckets'][env]
    S3_ERROR_FILE_PREFIX = 'unprocessed_transactions/' #fsv_param_dict['s3_buckets_and_folders']['folders']['unprocessed_transactions']
    
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    error_file_key = f"{S3_ERROR_FILE_PREFIX}{programName}_{dbname}_{timestamp}.csv"

    try:
        csv_buffer = io.StringIO()
        pd.DataFrame(failed_records).to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=FSV_S3_BUCKET_NAME, Key=error_file_key, Body=csv_buffer.getvalue())
        logging.info(f"Failed records saved to S3: {error_file_key}")
    except Exception as e:
        logging.error(f"Error saving failed records to S3: {e}")


import boto3
# Initialize AWS Clients
s3 = boto3.client("s3")

ssm = boto3.client("ssm")
def archive_processed_files(bucket, source_key, archive_folder):
    """Move processed files to an archive folder in S3"""
    archive_key = f"{archive_folder}/{source_key}"
    try:
        s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': source_key}, Key=archive_key)
        s3.delete_object(Bucket=bucket, Key=source_key)
        logging.info(f"File {source_key} archived to {archive_key}")
    except Exception as e:
        logging.error(f"Error archiving file {source_key}: {e}")        


import boto3
import json
import io
import re
import os
import psycopg2
import pandas as pd

# Initialize AWS Clients
s3 = boto3.client("s3")
ssm = boto3.client("ssm")

# Get Environment
env = "DEV" #os.environ.get("env")


def process_file(file_key, bucket, archivefolder, dynamoPaymentConfig, dynamoPrograms, sql_prg_db, sql_datax_db, fsv_uuid):
    # Connect to SSM
    #fsv_params = ssm.get_parameters(Names=["/BXRIS/ETL/ADF/fsvParams"], WithDecryption=True)
    #fsv_params_vals = fsv_params['Parameters'][0]['Value']
    #fsv_param_dict = json.loads(fsv_params_vals)

    # Fetch database connection details
    #db_config = fsv_param_dict['bbdb'][env]
    bb_dbHost, bb_dbName, bb_dbUser, bb_dbPort, bb_pwd = "bbdb.cluster-cxegro3a5uc0.us-east-1.rds.amazonaws.com", 'postgres', 'bxris', 5432, 'Oluwaboy1'
    #dx_config = fsv_param_dict['datax'][env]
    dbHost, dbName, dbUser, dbPort, dbPwd = 'dataxdb.cluster-cxegro3a5uc0.us-east-1.rds.amazonaws.com', 'postgres', 'bxris', 5432, "Oluwaboy1"

    """Processes an individual file from S3 and inserts it into PostgreSQL"""
    failed_records = []

    logging.info(f'Processing file on path: {file_key} from bucket:{bucket}')
    
    # extracted programName from file key 
    xtractFileKey = file_key.split("/")[-1]
    match = re.search(r'_(.+?)_split_file', xtractFileKey)
    xtracted_ProgramName = match.group(1) if match else None
    logging.info(f'Extracted Program Name: {xtracted_ProgramName}')
    logging.info(f'xtracted file key:{xtractFileKey}')

    #uuid_programName for unprocessed file naming
    matches = re.match(r'([A-Za-z0-9-]+)_([A-Za-z0-9-]+)_split_file', xtractFileKey)
    uuid = matches.group(1)
    prgName = matches.group(2)
    uuid_programName = f"{uuid}_{prgName}"
    logging.info(f'Extracted uuid_programName for unprocessed records file creation:{uuid_programName}')

    # Fetch file from S3
    response_obj = s3.get_object(Bucket=bucket, Key=file_key)
    csv_data = response_obj['Body'].read().decode('utf-8')

    # Determine delimiter based on file type
    delimiter = "|" if "split_file" in file_key else ","
    df = pd.read_csv(io.StringIO(csv_data), delimiter=delimiter, header=None)
    logging.info(f"CSV file {file_key} successfully retrieved and converted to DataFrame")

    try:
        """
        ########## Fetch Configuration ID from paymentCofig DDB
        response = dynamoPaymentConfig.scan(
            FilterExpression="configuration_json.paymentProcessorProgramName = :value",
            ExpressionAttributeValues={":value": xtracted_ProgramName}
        )
        if 'Items' in response and response['Items']:
            sub_group_num = response['Items'][0]['configuration_id']
        else:
            print("No matching records found for sub group number.")
            return None

        # call api to fetch the db program name and with that name fetch the connection string    

        # Fetch Program ID from DynamoDB
        response = dynamoPrograms.scan(
            FilterExpression="sub_group_number = :value",
            ExpressionAttributeValues={":value": sub_group_num}
        )

        ############ fetch db program name to find the connection string ############
        # API Gateway URL
        api_url = "https://your-api-id.execute-api.us-east-1.amazonaws.com/prod/resource"

        # Headers (if needed, e.g., API Key)
        headers = {
            "x-api-key": "your-api-key",  # Optional, if your API Gateway requires a key (sub_group_num)
            "Content-Type": "application/json"
        }
        # Make GET request
        response = requests.get(api_url, headers=headers)

        # Print response
        print("Status Code:", response.status_code)
        print("Response:", response.json())

        ######### get connection string from programs DDB ##########
        if 'Items' in response and response['Items']:
            conn_string = response['Items'][0]['connection_string_text']
        else:
            print("No matching records found for connection string.")
            return None

        matchDBname = re.search(r'Database=([^;]+)', conn_string)
        databaseName = matchDBname.group(1) if matchDBname else None

        if not databaseName:
            print("Database name extraction failed.")
            return None

        print("Database:", databaseName)
        
        """
        # Connect to PostgreSQL
        try:
            #token_bb = generateToken(env)
            #token_dx = generateToken(env)

            conn_bb = psycopg2.connect(host=bb_dbHost, port=bb_dbPort, database=bb_dbName, user=bb_dbUser, password=bb_pwd)
            conn_dx = psycopg2.connect(host=dbHost, port=dbPort, database=dbName, user=dbUser, password=dbPwd)

            cursor_bb, cursor_dx = conn_bb.cursor(), conn_dx.cursor()

            try:
                for row in df.itertuples(index=False, name=None):
                    formattedValues = ",".join(map(str, row))
                    row_as_string = f"{formattedValues}"
                    logging.info(f"rows to insert into db : {row_as_string}")

                    cursor_bb.execute(sql_prg_db, (xtracted_ProgramName, row_as_string, fsv_uuid))
                    cursor_dx.execute(sql_datax_db, (xtracted_ProgramName, row_as_string, fsv_uuid))

                conn_bb.commit()
                conn_dx.commit()
                logging.info(f"Data from folder: {file_key} successfully inserted into Postgres datax & B&B databases")

            except Exception as row_error:
                conn_bb.rollback()
                conn_dx.rollback()
                logging.error(f"Failed to insert row: {row_error}")
                failed_records.append(dict(zip(df.columns, row)))
                archive_failed_records(failed_records, uuid_programName, bb_dbName)

        except Exception as e:
            logging.error(f'Failed to connect to the database: {e}', 'error')

        finally:
            if cursor_bb is not None:
                cursor_bb.close()
            if cursor_dx is not None:
                cursor_dx.close()
            if conn_bb is not None:
                conn_bb.close()
            if conn_dx is not None:
                conn_dx.close()

    except Exception as e:
        logging.error(f"Error processing file {file_key}: {e}")


import boto3
import json
import os
import re

# Initialize AWS Clients
s3 = boto3.client("s3")
ssm = boto3.client("ssm")

# Get Environment
env = "DEV" #os.environ.get("env")

def process_FSVSplitFiles(event, context):
    # Connect to SSM
    #fsv_params = ssm.get_parameters(Names=["/BXRIS/ETL/ADF/fsvParams"], WithDecryption=True)
    #fsv_params_vals = fsv_params['Parameters'][0]['Value']
    #fsv_param_dict = json.loads(fsv_params_vals)

    # s3 Bucket details
    fsv_bucket = event['Records'][0]['s3']['bucket']['name']
    folderName = event['Records'][0]['s3']['object']['key'].split("/")[-2]
    bucketKey = event['Records'][0]['s3']['object']['key'].split("/")[-1]
    filePath = event['Records'][0]['s3']['object']['key']
    archivefolder = 'archive_files' # fsv_param_dict['s3_buckets_and_folders']['folders']['archive_files']
    logging.info(f'bucket: {fsv_bucket} , folder: {folderName}, key: {bucketKey}')

    # DynamoDB table configurations
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    dynamoPrograms = dynamodb.Table('bxris-programs') #dynamodb.Table(fsv_param_dict['DDBTables']['program'])
    dynamoPaymentConfig = dynamodb.Table('bxris-configuration') #dynamodb.Table(fsv_param_dict['DDBTables']['configuration'])

    # SQL Queries
    sql_datax_db = """SELECT * FROM internal.add_fsv_transaction_program_db(%s, %s, %s);"""
    sql_prg_db = """SELECT * FROM internal.add_fsv_transaction_program_db(%s, %s, %s);""" # fsv_param_dict['sql']['datax_insert_payload'],fsv_param_dict['sql']['programdb_insert_payload']

    # S3 Bucket details
    xtract_uuid = bucketKey
    match = re.search(r'^[a-zA-Z0-9\-]+', xtract_uuid)
    fsv_uuid = match.group(0) if match else None

    # Processing Split Files
    if folderName == 'success_split_files':
        success_file_key = filePath
        process_file(success_file_key, fsv_bucket, archivefolder, dynamoPaymentConfig, dynamoPrograms, sql_prg_db, sql_datax_db, fsv_uuid)
        logging.info(f'processed split files: {success_file_key} successfully')
            
    # Processing Unprocessed Files
    if folderName == 'unprocessed_transactions':
        unprocessed_transactions_file_key = filePath
        process_file(unprocessed_transactions_file_key, fsv_bucket, archivefolder, dynamoPaymentConfig, dynamoPrograms, sql_prg_db, sql_datax_db, fsv_uuid)
        logging.info(f'processed unprocessed transaction files: {unprocessed_transactions_file_key} successfully')

# Define a sample event to pass to the function
if __name__ == "__main__":
    process_FSVSplitFiles({"Records":[{"s3":{"bucket":{"name":"bxris-common-dev"},"object":{"key": "success_split_files/2e3789-4eg7-adc7-def2e33b90_Macaluso-Biooncology-C_split_file_20250112_121021.csv"}}}]}, None)


