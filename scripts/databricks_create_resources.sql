-- ==============================================================================
-- Databricks Unity Catalog & Environment Setup
-- Real-Time Transaction Fraud & Anomaly Detection Platform
-- ==============================================================================

-- 1. Create Unity Catalog
CREATE CATALOG IF NOT EXISTS fraud_dev;

-- 2. Create Medallion and Control Schemas
CREATE SCHEMA IF NOT EXISTS fraud_dev.staging
    COMMENT 'Staging area for incoming files and volume landing zones';

CREATE SCHEMA IF NOT EXISTS fraud_dev.bronze
    COMMENT 'Raw ingestion tables via Auto Loader and batch ingestion';

CREATE SCHEMA IF NOT EXISTS fraud_dev.silver
    COMMENT 'Cleaned, typed, deduplicated and feature-engineered datasets';

CREATE SCHEMA IF NOT EXISTS fraud_dev.gold
    COMMENT 'Scored transactions, risk alerts, and analytical business summaries';

CREATE SCHEMA IF NOT EXISTS fraud_dev.control
    COMMENT 'Pipeline run telemetry, audit logs, model status, and reconciliation';

CREATE SCHEMA IF NOT EXISTS fraud_dev.quarantine
    COMMENT 'Quarantined bad records, schema mismatches, and validation rejects';

CREATE SCHEMA IF NOT EXISTS fraud_dev.ml
    COMMENT 'Machine learning models, experiments, and model evaluation metrics';

-- 3. Create Unity Catalog Volume for File Ingestion & Checkpoints
CREATE VOLUME IF NOT EXISTS fraud_dev.staging.source_files
    COMMENT 'Volume landing directory for incoming CSV files and streaming checkpoints';

-- 4. Control Tables
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
USING DELTA
COMMENT 'Tracks file lifecycle across incoming, validation, processing, and archive';

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
USING DELTA
COMMENT 'Tracks end-to-end pipeline run executions and recovery state';

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
USING DELTA
COMMENT 'Stores layer-by-layer reconciliation audit records';

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
    failure_reason STRING,
    decision_threshold DOUBLE
)
USING DELTA
COMMENT 'Tracks MLflow registered models, active deployment status, and thresholds';
