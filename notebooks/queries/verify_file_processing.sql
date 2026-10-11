-- Databricks notebook source
-- DBTITLE 1,Audit File Processing Control Status
SELECT
    file_name,
    validation_status,
    processing_status,
    archived_at
FROM fraud_dev.control.file_processing_status
WHERE file_name = 'transactions_20261002_140100_001.csv'
ORDER BY discovered_at DESC;

-- COMMAND ----------

-- DBTITLE 1,Review Overall File Ingestion Status Summary
SELECT
    entity_name,
    validation_status,
    processing_status,
    count(*) AS file_count
FROM fraud_dev.control.file_processing_status
GROUP BY entity_name, validation_status, processing_status
ORDER BY entity_name;
