FROM apache/airflow:2.10.2

ARG AIRFLOW_VERSION=2.10.2
ARG PYTHON_VERSION=3.12

COPY requirements.txt /requirements.txt

# 1. Install Airflow providers using constraints
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# 2. Install dbt without constraints to avoid protobuf version clash
RUN pip install --no-cache-dir \
    "dbt-core==1.9.4" \
    "dbt-snowflake==1.9.4"

RUN mkdir -p \
    /opt/airflow/iomt_clinical_warehouse/analyses \
    /opt/airflow/iomt_clinical_warehouse/seeds \
    /opt/airflow/iomt_clinical_warehouse/snapshots
