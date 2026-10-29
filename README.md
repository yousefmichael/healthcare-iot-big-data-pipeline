## 📑 Table of Contents
- [Architecture Overview](#-architecture-overview)
- [Key Features](#-key-features)
- [Data Model & Medallion Design](#-data-model--medallion-design)
  - [Bronze Layer (Raw Ingestion)](#1-bronze-layer-raw-ingestion)
  - [Silver Layer (Data Cleansing & Quarantine)](#2-silver-layer-data-cleansing--quarantine)
  - [Gold Layer (Business Aggregations)](#3-gold-layer-business-aggregations)
  - [Warehouse & dbt Dimensional Modeling](#4-warehouse--dbt-dimensional-modeling)
- [Power BI Analytics & Executive Insights](#-power-bi-analytics--executive-insights)
- [Repository Structure](#-repository-structure)
- [Step-by-Step Runbook](#-step-by-step-runbook)
- [Testing & Data Quality Governance](#-testing--data-quality-governance)
- [Strategic Clinical Recommendations](#-strategic-clinical-recommendations)

---

## 🏗 System Architecture

```text
                                [ Hospital Clinical Environment ]
                                                │
               ┌────────────────────────────────┴────────────────────────────────┐
               │                                                                 │
    [ Batch Data Generator ]                                       [ Real-Time Kafka Producer ]
 (generate_batch_data_v2.py)                                          (stream_to_kafka.py)
  Wards, Beds, Patients, Devices,                                   Vital Signs & Device Events
    Admissions, Assignments                                                      │
               │                                                                 ▼
               │                                                      [ Apache Kafka Topics ]
               │                                                      - vital-readings
               │                                                      - device-status-events
               │                                                                 │
               │                                                                 ▼
               │                                                     [ S3 Streaming Bridge ]
               │                                                  (Micro-batch JSON flusher)
               │                                                                 │
               └───────────────────────────────┬─────────────────────────────────┘
                                               │
                                               ▼
                                 ┌───────────────────────────┐
                                 │     Amazon S3 Bucket      │
                                 │   (Raw Bronze Storage)    │
                                 │  - raw/batch/             │
                                 │  - raw/streaming/         │
                                 └─────────────┬─────────────┘
                                               │
                                               ▼
                                ┌─────────────────────────────┐
                                │      AWS Glue / PySpark     │
                                │   (Medallion Lakehouse)     │
                                ├─────────────────────────────┤
                                │ • Missing Value Checks      │
                                │ • SHA-256 Deduplication     │
                                │ • Referential FK Validation │ ────► [ Quarantine Audit Layer ]
                                │ • Physiological Range Gate  │       (unified_removed_records)
                                └──────────────┬──────────────┘
                                               │
                                               ▼
                                ┌─────────────────────────────┐
                                │   Amazon S3 Silver & Gold   │
                                │  - silver/ (Clean Parquet)  │
                                │  - gold/ (Analytics Parquet)│
                                └──────────────┬──────────────┘
                                               │
                                               ▼
                                ┌─────────────────────────────┐
                                │   Snowflake Data Warehouse  │
                                │   (Storage Integration)     │
                                ├─────────────────────────────┤
                                │ Schema: RAW                 │
                                │ Row-Count Zero-Trap Gates   │
                                └──────────────┬──────────────┘
                                               │
                                               ▼
                                ┌─────────────────────────────┐
                                │     dbt Core (96 Tests)     │
                                ├─────────────────────────────┤
                                │ • STAGING     (9 Views)     │
                                │ • INTERMEDIATE(2 Views)     │
                                │ • MARTS       (12 Tables)   │
                                └──────────────┬──────────────┘
                                               │
                                               ▼
                                ┌─────────────────────────────┐
                                │      Power BI Analytics     │
                                ├─────────────────────────────┤
                                │ 1. Hospital Operations      │
                                │ 2. Vital Readings & Anomaly │
                                │ 3. IoT Device Maintenance   │
                                └─────────────────────────────┘