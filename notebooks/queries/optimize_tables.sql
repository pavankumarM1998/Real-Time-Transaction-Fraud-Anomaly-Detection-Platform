-- Databricks notebook source
-- DBTITLE 1,Optimize Delta Tables for High-Performance Queries
OPTIMIZE fraud_dev.silver.transactions;
OPTIMIZE fraud_dev.silver.transaction_fraud_scored;
OPTIMIZE fraud_dev.gold.fraud_transaction_decisions;

-- COMMAND ----------

-- DBTITLE 1,Inspect Table Details and Metrics
DESCRIBE DETAIL fraud_dev.silver.transactions;

-- COMMAND ----------

DESCRIBE DETAIL fraud_dev.silver.transaction_fraud_scored;

-- COMMAND ----------

DESCRIBE DETAIL fraud_dev.gold.fraud_transaction_decisions;
