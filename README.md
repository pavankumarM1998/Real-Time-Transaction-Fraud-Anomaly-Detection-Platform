# Real-Time Transaction Fraud & Anomaly Detection Platform

A production-grade, enterprise Lakehouse platform for real-time transaction processing, anomaly detection, rolling velocity feature calculation, and low-latency machine learning inference powered by **Databricks**, **Apache Spark Structured Streaming**, **Delta Lake**, **Unity Catalog**, and **MLflow**.

---

## 🏗️ Architecture & Component Overview

The platform implements a unified streaming and batch Medallion Lakehouse architecture designed for zero data loss, sub-second anomaly detection, dynamic threshold routing, and full auditability.

```
Real-Time-Transaction-Fraud-Anomaly-Detection-Platform/
│
├── notebooks/                              # End-to-end Databricks Notebooks (Staging -> Gold)
│   ├── 00_project_setup.sql                # Unity Catalog, schemas, volume, and control DDL
│   ├── 01_staging_validation.py            # File ingestion schema validation & quarantine routing
│   ├── 02_reference_ingestion.py           # Bronze batch ingestion (customers, merchants, fraud labels)
│   ├── 03_bronze_transaction_stream.py     # Auto Loader streaming ingestion for transactions
│   ├── 04_silver_transaction_stream.py     # Silver micro-batch streaming, typing & quarantine rejects
│   ├── 05_fraud_processing.py             # Streaming fraud rules & 30-minute watermarking
│   ├── 06_reference_silver.py              # Silver reference entity cleansing & deduplication
│   ├── 07_transaction_enrichment.py        # Silver transaction enrichment with customer & merchant risk
│   ├── 08_fraud_feature_assembly.py        # Customer rolling velocity windows (5m & 10m) & feature store
│   ├── 09_ml_training.py                   # MLflow model training baseline (PR-AUC evaluation)
│   ├── 09_ml_training_v2.py                # Enhanced MLflow training, threshold tuning & Unity Catalog registry
│   ├── 10_streaming_inference.py           # Real-time streaming model scoring via active model registry
│   ├── 11_gold_fraud.py                    # Gold decision layer (BLOCK / REVIEW / ALLOW) & aggregations
│   ├── 12_reconciliation_archive.py       # Cross-layer mathematical reconciliation & file archiving
│   ├── 13_failure_recovery_tests.py        # Fault tolerance replay tests & pipeline idempotency checks
│   ├── 14_monitoring.py                    # Operational telemetry dashboard & stuck file auditing
│   └── queries/                            # Maintenance and inspection queries
│       ├── optimize_tables.sql             # Delta Lake table compaction & OPTIMIZE commands
│       └── verify_file_processing.sql      # Ingestion status and reconciliation verification
│
├── resources/                              # Databricks Asset Bundle (DAB) Workflow / Job YAMLs
│   ├── fraud_setup.job.yml                 # Environment & Unity Catalog initialization workflow
│   ├── fraud_staging_reference.job.yml     # Reference data ingestion & staging validation workflow
│   ├── fraud_streaming_bronze_silver.job.yml # Real-time streaming Bronze & Silver workflow
│   ├── fraud_feature_enrichment.job.yml    # Feature enrichment & velocity windows workflow
│   ├── fraud_ml_inference_gold.job.yml     # Model training, real-time inference & Gold build workflow
│   ├── fraud_reconciliation_archive.job.yml # Cross-layer reconciliation & monitoring workflow
│   └── fraud_master_pipeline.job.yml       # End-to-end master orchestration pipeline
│
├── scripts/                                # Provisioning & environment setup scripts
│   ├── databricks_create_resources.sql     # Standalone SQL DDL for Catalogs, Schemas & Control tables
│   └── setup_folder_resources.py           # Unity Catalog Volume folder structure initializer
│
├── sample_data/                            # Sample schema-valid test datasets
│   ├── customers_20261002.csv              # Customer demographics & historical spend benchmarks
│   ├── merchants_20261002.csv              # Merchant categories, chargeback rates & fraud history
│   ├── fraud_labels_20261002.csv           # Model training dataset with fraud labels
│   ├── transactions_20261002_140100_001.csv # Real-time transaction streaming test batch
│   └── README.md                           # Sample data usage and upload instructions
│
├── docs/                                   # Architectural specifications & data dictionaries
│   ├── architecture.md                     # Detailed platform flow, watermarking & MLflow lifecycle
│   └── data_dictionary.md                  # Comprehensive schema documentation for all tables
│
├── .github/
│   └── workflows/
│       └── databricks_bundle_ci.yml        # CI/CD pipeline for bundle validation and syntax checks
│
├── databricks.yml                          # Databricks Asset Bundle (DAB) configuration manifest
├── .gitignore                              # Production gitignore for Databricks, Python & Delta Lake
└── README.md                               # Platform documentation
```

---

## 🌟 Key Features & Capabilities

### 1. Medallion Lakehouse Processing
* **Staging Area**: Secure landing zones in **Unity Catalog Volumes** (`/Volumes/fraud_dev/staging/source_files/`) for raw incoming batch feeds, CSV streams, and streaming checkpoints.
* **Bronze Layer**: Append-only raw ingestion via **Databricks Auto Loader** (`cloudFiles`) with automated schema evolution, rescue columns, and zero duplicate ingestion.
* **Silver Layer**: Cleansed, typed, and deduplicated domain entities. Bad records failing constraints are quarantined in `fraud_dev.quarantine.transaction_rejects`.
* **Gold Layer**: Curated business decisions in `fraud_dev.gold.fraud_transaction_decisions` categorized into **BLOCK**, **REVIEW**, and **ALLOW** tiers, alongside merchant and customer risk summaries.

### 2. Real-Time Streaming & Watermarking
* **Structured Streaming**: Micro-batch processing from Bronze to Silver with stateful streaming deduplication.
* **Watermarking**: 30-minute late-data watermark (`.withWatermark("transaction_timestamp", "30 minutes")`) to ensure deterministic deduplication while preventing unbounded state memory expansion.
* **Deterministic Rules**: Low-latency deterministic rule engines evaluate instant high-value thresholds (`amount >= 50,000`) before ML inference.

### 3. Velocity Windows & Feature Store
* **Sliding Customer Velocity**: Calculates real-time windowed features per customer:
  * **5-Minute Window**: Instant transaction count (`txn_count_5m`) and cumulative amount (`txn_total_amount_5m`).
  * **10-Minute Window**: Extended burst velocity (`txn_count_10m`) and cumulative amount (`txn_total_amount_10m`).
* **Behavioral Ratios**: Dynamically evaluates `amount_vs_customer_avg` comparing current transaction amounts to historical baseline averages.

### 4. MLflow Model Governance & Dynamic Inference
* **Experiment Tracking & Registry**: Models trained on historical fraud patterns are tracked with MLflow, optimized for Precision-Recall AUC (PR-AUC), and registered in Unity Catalog (`fraud_dev.ml.fraud_detection_model`).
* **Dynamic Active Model Resolution**: Scoring jobs dynamically query `fraud_dev.control.model_status` to fetch the current active model version and optimal decision threshold—enabling **zero-downtime model deployments**.
* **Model Version Lifecycle**: Safe, atomic version activation: newly approved models are activated while older versions are cleanly transitioned to `INACTIVE`.

### 5. Enterprise Control, Reconciliation & Fault Recovery
* **Cross-Layer Balance Reconciliation**: Verifies that every transaction is accounted for:
  $$\text{Bronze Count} = \text{Silver Count} + \text{Quarantine Count}$$
* **Automated Archiving**: Processed raw CSV files are atomically moved from `incoming/` to `archive/` after successful reconciliation.
* **Idempotent Replay Testing**: Validates pipeline recovery resilience across simulated node failures and network interruptions.

---

## 🚀 Quick Start Guide

### Step 1: Provision Unity Catalog Resources
Run the environment creation script in the Databricks SQL Editor or execute as a notebook:
```sql
-- Run in Databricks SQL Editor
run scripts/databricks_create_resources.sql;
```

### Step 2: Initialize Volume Landing Subdirectories
Execute the directory setup script to create all volume landing zones and checkpoint paths:
```python
# Run in a Databricks Python Notebook
%run ./scripts/setup_folder_resources.py
```

### Step 3: Upload Sample Data
Upload the provided test CSV files to their respective volume directories using the Databricks CLI:
```bash
databricks fs cp sample_data/customers_20261002.csv \
  dbfs:/Volumes/fraud_dev/staging/source_files/customers/incoming/customers_20261002.csv

databricks fs cp sample_data/merchants_20261002.csv \
  dbfs:/Volumes/fraud_dev/staging/source_files/merchants/incoming/merchants_20261002.csv

databricks fs cp sample_data/fraud_labels_20261002.csv \
  dbfs:/Volumes/fraud_dev/staging/source_files/fraud_labels/incoming/fraud_labels_20261002.csv

databricks fs cp sample_data/transactions_20261002_140100_001.csv \
  dbfs:/Volumes/fraud_dev/staging/source_files/transactions/incoming/transactions_20261002_140100_001.csv
```

### Step 4: Deploy & Run with Databricks Asset Bundles (DAB)
Deploy the platform workflows to your Databricks workspace using DAB:
```bash
# Validate bundle configuration
databricks bundle validate -t dev

# Deploy workflows
databricks bundle deploy -t dev

# Trigger the End-to-End Master Pipeline
databricks bundle run fraud_master_pipeline_job -t dev
```

---

## 📊 Pipeline Execution Sequence

For manual execution or sequential testing across notebooks:

| Step | Notebook | Description | Output Table / Target |
|:---:|:---|:---|:---|
| **00** | `00_project_setup.sql` | Catalog, schema, volume & control table creation | `fraud_dev.*` |
| **01** | `01_staging_validation.py` | Schema, mandatory columns & filename pattern checks | `fraud_dev.control.file_processing_status` |
| **02** | `02_reference_ingestion.py` | Ingestion of reference data (`customers`, `merchants`, `labels`) | `fraud_dev.bronze.*` |
| **03** | `03_bronze_transaction_stream.py` | Auto Loader streaming transaction ingestion | `fraud_dev.bronze.transactions` |
| **04** | `04_silver_transaction_stream.py` | Micro-batch schema enforcement & quarantine routing | `fraud_dev.silver.transactions` |
| **05** | `05_fraud_processing.py` | 30m watermarking, deduplication & high-amount rule | `fraud_dev.silver.transaction_fraud_features` |
| **06** | `06_reference_silver.py` | Reference data cleansing, casting & deduplication | `fraud_dev.silver.customers`, `merchants` |
| **07** | `07_transaction_enrichment.py` | Join transactions with customer & merchant risk scores | `fraud_dev.silver.transaction_enriched_features` |
| **08** | `08_fraud_feature_assembly.py` | Compute 5m & 10m sliding velocity windows | `fraud_dev.silver.transaction_complete_fraud_features`|
| **09** | `09_ml_training_v2.py` | Train Logistic Regression model & register in Unity Catalog | `fraud_dev.ml.fraud_detection_model` |
| **10** | `10_streaming_inference.py` | Dynamic model resolution & real-time fraud scoring | `fraud_dev.silver.transaction_fraud_scored` |
| **11** | `11_gold_fraud.py` | Generate decision tiers (`BLOCK`, `REVIEW`, `ALLOW`) | `fraud_dev.gold.fraud_transaction_decisions` |
| **12** | `12_reconciliation_archive.py` | Cross-layer mathematical reconciliation & file archiving | `fraud_dev.control.reconciliation_results` |
| **13** | `13_failure_recovery_tests.py` | Fault tolerance replay tests & idempotency validation | Verification Reports |
| **14** | `14_monitoring.py` | Operational telemetry, stuck file audits & failure logs | Telemetry Dashboard |

---

## 🔒 Security & Governance

* **Unity Catalog Data Isolation**: Separate schemas (`staging`, `bronze`, `silver`, `gold`, `control`, `quarantine`, `ml`) enforce role-based access control (RBAC).
* **Data Lineage**: Complete automatic lineage tracking from raw landing files down to final gold decision tables.
* **Delta Lake ACID Guarantees**: Guarantees read consistency during continuous concurrent streaming writes and batch ML retraining.