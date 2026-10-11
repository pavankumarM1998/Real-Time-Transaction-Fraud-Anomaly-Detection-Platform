# Databricks notebook source
from pyspark.sql import functions as F

latest_run_df = (
    spark.table("fraud_dev.control.pipeline_runs")
    .orderBy(F.col("end_timestamp").desc())
    .limit(10)
)

display(latest_run_df)

# COMMAND ----------

# DBTITLE 1,Latest reconciliation failures
recon_failures_df = (
    spark.table(
        "fraud_dev.control.reconciliation_results"
    )
    .filter(
        F.col("reconciliation_status") == "FAIL"
    )
    .orderBy(
        F.col("reconciliation_timestamp").desc()
    )
)

display(recon_failures_df)

# COMMAND ----------

# DBTITLE 1,Files stuck before archive
stuck_files_df = (
    spark.table(
        "fraud_dev.control.file_processing_status"
    )
    .filter(
        (F.col("validation_status") == "PASS")
        &
        (
            F.col("processing_status")
            .isin(
                "READY",
                "BRONZE_LOADED",
                "SILVER_LOADED",
                "PROCESSED"
            )
        )
    )
)

display(stuck_files_df)

# COMMAND ----------

# DBTITLE 1,Rejected files
rejected_files_df = (
    spark.table(
        "fraud_dev.control.file_processing_status"
    )
    .filter(
        F.col("processing_status") == "REJECTED"
    )
    .orderBy(
        F.col("validated_at").desc()
    )
)

display(rejected_files_df)

# COMMAND ----------

# DBTITLE 1,Active model validation
active_models_df = (
    spark.table(
        "fraud_dev.control.model_status"
    )
    .filter(
        (F.col("is_active") == True)
        &
        (F.col("model_status") == "APPROVED")
    )
)

active_model_count = active_models_df.count()

print(
    "Active approved model count:",
    active_model_count
)

if active_model_count != 1:
    raise Exception(
        f"MODEL_MONITORING_FAILURE: "
        f"active model count = {active_model_count}"
    )

# COMMAND ----------

# DBTITLE 1,Then create a simple operational summary
failed_pipeline_runs = (
    spark.table(
        "fraud_dev.control.pipeline_runs"
    )
    .filter(
        F.col("run_status") == "FAILED"
    )
    .count()
)

failed_reconciliations = (
    spark.table(
        "fraud_dev.control.reconciliation_results"
    )
    .filter(
        F.col("reconciliation_status") == "FAIL"
    )
    .count()
)

stuck_file_count = (
    stuck_files_df.count()
)

rejected_file_count = (
    rejected_files_df.count()
)

print("Failed pipeline runs     :", failed_pipeline_runs)
print("Failed reconciliations   :", failed_reconciliations)
print("Stuck files              :", stuck_file_count)
print("Rejected files           :", rejected_file_count)
print("Active approved models   :", active_model_count)