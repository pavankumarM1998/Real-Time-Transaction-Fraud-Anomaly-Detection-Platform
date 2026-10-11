-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS fraud_dev;

CREATE SCHEMA IF NOT EXISTS fraud_dev.staging;
CREATE SCHEMA IF NOT EXISTS fraud_dev.bronze;
CREATE SCHEMA IF NOT EXISTS fraud_dev.silver;
CREATE SCHEMA IF NOT EXISTS fraud_dev.gold;
CREATE SCHEMA IF NOT EXISTS fraud_dev.control;
CREATE SCHEMA IF NOT EXISTS fraud_dev.quarantine;

CREATE SCHEMA IF NOT EXISTS fraud_dev.ml;

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS fraud_dev.staging.source_files;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC base = "/Volumes/fraud_dev/staging/source_files"
-- MAGIC
-- MAGIC folders = [
-- MAGIC     "transactions/incoming",
-- MAGIC     "transactions/rejected",
-- MAGIC     "transactions/archive",
-- MAGIC
-- MAGIC     "customers/incoming",
-- MAGIC     "customers/rejected",
-- MAGIC     "customers/archive",
-- MAGIC
-- MAGIC     "merchants/incoming",
-- MAGIC     "merchants/rejected",
-- MAGIC     "merchants/archive",
-- MAGIC
-- MAGIC     "fraud_labels/incoming",
-- MAGIC     "fraud_labels/rejected",
-- MAGIC     "fraud_labels/archive",
-- MAGIC
-- MAGIC     "checkpoints/bronze_transactions",
-- MAGIC     "checkpoints/silver_transactions",
-- MAGIC     "checkpoints/fraud_processing"
-- MAGIC ]
-- MAGIC
-- MAGIC for folder in folders:
-- MAGIC     path = f"{base}/{folder}"
-- MAGIC     dbutils.fs.mkdirs(path)
-- MAGIC     print("Created:", path)

-- COMMAND ----------

-- DBTITLE 1,File processing control
CREATE TABLE IF NOT EXISTS fraud_dev.control.file_processing_status
(
    file_path STRING,
    file_name STRING,
    entity_name STRING,
    file_size BIGINT,
    file_modification_time TIMESTAMP,

    validation_status STRING,
    processing_status STRING,

    row_count BIGINT,

    pipeline_run_id STRING,

    failure_reason STRING,

    discovered_at TIMESTAMP,
    validated_at TIMESTAMP,
    processed_at TIMESTAMP,
    archived_at TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,Pipeline run control
CREATE TABLE IF NOT EXISTS fraud_dev.control.pipeline_runs
(
    pipeline_run_id STRING,
    pipeline_name STRING,

    run_status STRING,

    start_timestamp TIMESTAMP,
    end_timestamp TIMESTAMP,

    last_successful_layer STRING,
    failure_layer STRING,
    failure_reason STRING,

    recovery_status STRING
)
USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,Reconciliation table
CREATE TABLE IF NOT EXISTS fraud_dev.control.reconciliation_results
(
    pipeline_run_id STRING,
    entity_name STRING,
    layer_name STRING,

    source_count BIGINT,
    target_count BIGINT,
    quarantine_count BIGINT,
    duplicate_count BIGINT,

    reconciliation_status STRING,
    reconciliation_timestamp TIMESTAMP,

    failure_reason STRING
)
USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,Model status table
CREATE TABLE IF NOT EXISTS fraud_dev.control.model_status
(
    model_name STRING,
    model_version STRING,

    model_status STRING,
    is_active BOOLEAN,

    validation_metric_name STRING,
    validation_metric_value DOUBLE,

    registered_at TIMESTAMP,
    activated_at TIMESTAMP,

    failure_reason STRING
)
USING DELTA;

-- COMMAND ----------

SHOW SCHEMAS IN fraud_dev;

-- COMMAND ----------

SHOW TABLES IN fraud_dev.control;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC     dbutils.fs.ls(
-- MAGIC         "/Volumes/fraud_dev/staging/source_files/"
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC base = "/Volumes/fraud_dev/staging/source_files"
-- MAGIC entity = [
-- MAGIC     "customers",
-- MAGIC     "merchants",
-- MAGIC     "fraud_labels",
-- MAGIC     "transactions"
-- MAGIC ]
-- MAGIC for En in entity:
-- MAGIC     display(dbutils.fs.ls(f"{base}/{En}/incoming/"))
-- MAGIC