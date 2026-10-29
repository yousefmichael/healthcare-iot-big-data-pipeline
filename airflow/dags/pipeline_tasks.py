import logging
import os
import subprocess
import tempfile
from pathlib import Path

from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_DATABASE = "IOMT_LAKEHOUSE_DB"
SNOWFLAKE_RAW_SCHEMA = "RAW"
DBT_PROFILE_NAME = "iomt_clinical_warehouse"

RAW_TABLES = (
    {
        "table": "RAW_WARDS",
        "stage": "IOMT_SILVER_STAGE",
        "prefix": "wards",
        "columns": ("ward_id", "ward_name", "floor", "ward_type"),
    },
    {
        "table": "RAW_BEDS",
        "stage": "IOMT_SILVER_STAGE",
        "prefix": "beds",
        "columns": ("bed_id", "ward_id", "bed_number", "bed_status"),
    },
    {
        "table": "RAW_PATIENTS",
        "stage": "IOMT_SILVER_STAGE",
        "prefix": "patients",
        "columns": ("patient_id", "name", "date_of_birth", "gender"),
    },
    {
        "table": "RAW_ADMISSIONS",
        "stage": "IOMT_SILVER_STAGE",
        "prefix": "admissions",
        "columns": (
            "admission_id",
            "patient_id",
            "bed_id",
            "admitted_at",
            "discharged_at",
            "status",
        ),
    },
    {
        "table": "RAW_DEVICES",
        "stage": "IOMT_SILVER_STAGE",
        "prefix": "devices",
        "columns": ("device_id", "device_type", "model", "installed_at"),
    },
    {
        "table": "RAW_DEVICE_ASSIGNMENTS",
        "stage": "IOMT_SILVER_STAGE",
        "prefix": "device_assignments",
        "columns": (
            "assignment_id",
            "device_id",
            "patient_id",
            "admission_id",
            "assigned_at",
            "unassigned_at",
        ),
    },
    {
        "table": "RAW_DEVICE_STATUS_EVENTS",
        "stage": "IOMT_SILVER_STAGE",
        "prefix": "device_status_events",
        "columns": ("device_id", "event_id", "event_time", "event_type", "partition"),
    },
    {
        "table": "RAW_VITAL_READINGS",
        "stage": "IOMT_SILVER_STAGE",
        "prefix": "vital_readings",
        "columns": (
            "device_id",
            "measured_at",
            "metric_name",
            "metric_value",
            "patient_id",
            "reading_id",
            "unit",
            "partition",
            "reading_status",
        ),
    },
    {
        "table": "RAW_VITAL_RULES_SUMMARY",
        "stage": "IOMT_GOLD_STAGE",
        "prefix": "vital_rules_summary",
        "columns": (
            "rule_id",
            "metric_name",
            "unit",
            "rule_type",
            "warning_operator",
            "warning_threshold",
            "critical_operator",
            "critical_threshold",
            "description",
        ),
    },
)


def refresh_snowflake_raw(snowflake_conn_id=SNOWFLAKE_CONN_ID):
    """Load the latest S3 snapshot, then atomically replace all nine RAW tables."""
    connection = SnowflakeHook(snowflake_conn_id=snowflake_conn_id).get_conn()
    cursor = connection.cursor()
    transaction_started = False
    staged_counts = {}
    try:
        cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_RAW_SCHEMA}")

        for item in RAW_TABLES:
            temp_table = f"TMP_{item['table']}"
            cursor.execute(
                f"CREATE TEMPORARY TABLE {temp_table} "
                f"LIKE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_RAW_SCHEMA}.{item['table']}"
            )
            cursor.execute(
                f"COPY INTO {temp_table} "
                f"FROM @{SNOWFLAKE_DATABASE}.{SNOWFLAKE_RAW_SCHEMA}."
                f"{item['stage']}/{item['prefix']}/ "
                f"FILE_FORMAT = (FORMAT_NAME = "
                f"'{SNOWFLAKE_DATABASE}.{SNOWFLAKE_RAW_SCHEMA}.IOMT_PARQUET_FORMAT') "
                "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE "
                "PATTERN = '.*\\.parquet$' FORCE = TRUE "
                "ON_ERROR = ABORT_STATEMENT"
            )
            cursor.execute(f"SELECT COUNT(*) FROM {temp_table}")
            row_count = cursor.fetchone()[0]
            if row_count == 0:
                raise RuntimeError(
                    f"No rows were loaded for {item['table']}; "
                    "the existing RAW snapshot was not replaced."
                )
            staged_counts[item["table"]] = row_count
            logging.info("Staged %s rows for %s", row_count, item["table"])

        connection.autocommit(False)
        cursor.execute("BEGIN")
        transaction_started = True
        for item in RAW_TABLES:
            column_list = ", ".join(item["columns"])
            cursor.execute(
                f"DELETE FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_RAW_SCHEMA}."
                f"{item['table']}"
            )
            cursor.execute(
                f"INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_RAW_SCHEMA}."
                f"{item['table']} ({column_list}) "
                f"SELECT {column_list} FROM TMP_{item['table']}"
            )
            cursor.execute(
                f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}."
                f"{SNOWFLAKE_RAW_SCHEMA}.{item['table']}"
            )
            refreshed_count = cursor.fetchone()[0]
            if refreshed_count != staged_counts[item["table"]]:
                raise RuntimeError(
                    f"RAW refresh count mismatch for {item['table']}: "
                    f"staged {staged_counts[item['table']]}, found {refreshed_count}."
                )
        connection.commit()
        transaction_started = False
        logging.info("Replaced all nine Snowflake RAW tables from the current S3 snapshot.")
    except Exception:
        if transaction_started:
            connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


def run_dbt_build(
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    project_dir="/opt/airflow/iomt_clinical_warehouse",
):
    """Run dbt dependencies and models/tests using credentials from Airflow."""
    airflow_connection = BaseHook.get_connection(snowflake_conn_id)
    extra = airflow_connection.extra_dejson
    account = airflow_connection.host or extra.get("account")
    database = extra.get("database") or airflow_connection.schema
    warehouse = extra.get("warehouse")
    role = extra.get("role")
    schema = airflow_connection.schema or "STAGING"

    required_settings = {
        "account": account,
        "user": airflow_connection.login,
        "password": airflow_connection.password,
        "database": database,
        "warehouse": warehouse,
        "role": role,
    }
    missing_settings = [name for name, value in required_settings.items() if not value]
    if missing_settings:
        raise ValueError(
            "The Airflow Snowflake connection is missing required settings: "
            + ", ".join(missing_settings)
        )

    environment = os.environ.copy()
    environment.update(
        {
            "DBT_SNOWFLAKE_ACCOUNT": account,
            "DBT_SNOWFLAKE_USER": airflow_connection.login,
            "DBT_SNOWFLAKE_PASSWORD": airflow_connection.password,
            "DBT_SNOWFLAKE_ROLE": role,
            "DBT_SNOWFLAKE_DATABASE": database,
            "DBT_SNOWFLAKE_WAREHOUSE": warehouse,
            "DBT_SNOWFLAKE_SCHEMA": schema,
        }
    )
    profile = f"""\
{DBT_PROFILE_NAME}:
  target: airflow
  outputs:
    airflow:
      type: snowflake
      account: "{{{{ env_var('DBT_SNOWFLAKE_ACCOUNT') }}}}"
      user: "{{{{ env_var('DBT_SNOWFLAKE_USER') }}}}"
      password: "{{{{ env_var('DBT_SNOWFLAKE_PASSWORD') }}}}"
      role: "{{{{ env_var('DBT_SNOWFLAKE_ROLE') }}}}"
      database: "{{{{ env_var('DBT_SNOWFLAKE_DATABASE') }}}}"
      warehouse: "{{{{ env_var('DBT_SNOWFLAKE_WAREHOUSE') }}}}"
      schema: "{{{{ env_var('DBT_SNOWFLAKE_SCHEMA') }}}}"
      threads: 4
"""

    project_path = Path(project_dir)
    if not (project_path / "dbt_project.yml").is_file():
        raise FileNotFoundError(f"dbt project file not found under {project_path}")

    with tempfile.TemporaryDirectory(prefix="airflow-dbt-profile-") as profile_dir:
        Path(profile_dir, "profiles.yml").write_text(profile, encoding="utf-8")
        for command in ("deps", "build"):
            subprocess.run(
                [
                    "dbt",
                    command,
                    "--project-dir",
                    str(project_path),
                    "--profiles-dir",
                    profile_dir,
                ],
                check=True,
                env=environment,
            )
