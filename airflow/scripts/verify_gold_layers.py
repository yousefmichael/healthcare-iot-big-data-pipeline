import os
import sys
import boto3

BUCKET = "healthcare-iot-lake"
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
s3 = boto3.client("s3", region_name=REGION)

tables = [
    "ward_occupancy_summary",
    "device_reliability_summary",
    "admission_summary",
    "device_incident_summary",
    "vital_rules_summary",
]

print(f"Connecting to S3 bucket '{BUCKET}' in region '{REGION}'...")
missing_tables = []

for table in tables:
    prefix = f"gold/{table}/"
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    contents = response.get("Contents", [])
    parquet_files = [obj["Key"] for obj in contents if obj["Key"].endswith(".parquet")]

    if parquet_files:
        print(f"✔ Found {table}: {len(parquet_files)} parquet file(s)")
    else:
        print(f"✖ Missing or empty: {prefix}", file=sys.stderr)
        missing_tables.append(table)

if missing_tables:
    print(f"ERROR: The following Gold tables are missing in S3: {missing_tables}", file=sys.stderr)
    sys.exit(1)

print("\nAll 5 Gold tables verified successfully in S3. Ready for Snowflake ingestion.")
