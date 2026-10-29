import os
import sys
import boto3
from botocore.exceptions import NoCredentialsError

BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "healthcare-iot-lake")
SOURCE_DIR = "/opt/airflow/generated_data"
S3_PREFIX = "raw/batch"

# Exactly the 6 dimension tables to upload
TARGET_TABLES = {
    "admissions",
    "beds",
    "device_assignments",
    "devices",
    "patients",
    "wards",
}

def upload_batch_files():
    s3 = boto3.client("s3")
    
    if not os.path.exists(SOURCE_DIR):
        print(f"Directory {SOURCE_DIR} does not exist. Run data generation first.")
        sys.exit(1)

    uploaded_count = 0
    for root, _, files in os.walk(SOURCE_DIR):
        if "evaluation_only" in root:
            continue
            
        for file in files:
            table_name, ext = os.path.splitext(file)
            
            # Filter strictly for the 6 target tables
            if table_name not in TARGET_TABLES or ext.lower() != ".csv":
                continue

            local_file_path = os.path.join(root, file)
            s3_key = f"{S3_PREFIX}/{table_name}/{file}"

            print(f"Uploading {file} -> s3://{BUCKET_NAME}/{s3_key}...")
            try:
                s3.upload_file(local_file_path, BUCKET_NAME, s3_key)
                uploaded_count += 1
            except NoCredentialsError:
                print("AWS credentials not found. Verify your environment variables.")
                sys.exit(1)
            except Exception as e:
                print(f"Failed to upload {file}: {e}")
                sys.exit(1)

    print(f"\nDone! Successfully uploaded {uploaded_count}/6 tables into s3://{BUCKET_NAME}/{S3_PREFIX}/")

if __name__ == "__main__":
    upload_batch_files()
