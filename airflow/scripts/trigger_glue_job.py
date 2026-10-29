import os
import sys
import time
import boto3

# Configuration from environment variables or defaults
GLUE_JOB_NAME = os.getenv("GLUE_JOB_NAME", "healthcare_iot_bronze_to_gold")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

print(f"Connecting to AWS Glue in region: {AWS_REGION}")
glue = boto3.client("glue", region_name=AWS_REGION)

def trigger_and_wait():
    print(f"Submitting job run request for Glue Job: '{GLUE_JOB_NAME}'...")
    try:
        response = glue.start_job_run(JobName=GLUE_JOB_NAME)
        run_id = response["JobRunId"]
        print(f"Successfully triggered! JobRunId: {run_id}")
    except Exception as e:
        print(f"Failed to start Glue Job '{GLUE_JOB_NAME}': {e}", file=sys.stderr)
        sys.exit(1)

    poll_interval_seconds = 15
    while True:
        time.sleep(poll_interval_seconds)
        try:
            status_response = glue.get_job_run(JobName=GLUE_JOB_NAME, RunId=run_id)
            job_run = status_response["JobRun"]
            state = job_run["JobRunState"]
            execution_time = job_run.get("ExecutionTime", 0)

            print(f"Status: {state} (Elapsed: {execution_time}s)")

            if state == "SUCCEEDED":
                print(f"AWS Glue Job '{GLUE_JOB_NAME}' completed successfully.")
                break
            elif state in ["FAILED", "STOPPED", "TIMEOUT"]:
                error_msg = job_run.get("ErrorMessage", "No error message provided.")
                print(f"Glue Job ended with state: {state}", file=sys.stderr)
                print(f"Error detail: {error_msg}", file=sys.stderr)
                sys.exit(1)

        except Exception as e:
            print(f"Error checking job status: {e}", file=sys.stderr)
            sys.exit(1)

if __name__ == "__main__":
    trigger_and_wait()
