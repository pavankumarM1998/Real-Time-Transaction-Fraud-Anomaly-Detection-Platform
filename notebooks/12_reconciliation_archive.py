# Databricks notebook source
# DBTITLE 1,Create a reconciliation run ID
from pyspark.sql import functions as F
from datetime import datetime
import uuid

RECON_RUN_ID = str(uuid.uuid4())

print("Reconciliation Run ID:", RECON_RUN_ID)

# COMMAND ----------

# DBTITLE 1,Collect transaction counts
bronze_df = spark.table(
    "fraud_dev.bronze.transactions"
)

silver_df = spark.table(
    "fraud_dev.silver.transactions"
)

gold_df = spark.table(
    "fraud_dev.gold.fraud_transaction_decisions"
)

quarantine_df = spark.table(
    "fraud_dev.quarantine.transaction_rejects"
)

# COMMAND ----------

# DBTITLE 1,Cell 3
bronze_count = bronze_df.count()

silver_count = silver_df.count()

gold_count = gold_df.count()

gold_distinct_count = (
    gold_df
    .select("transaction_id")
    .distinct()
    .count()
)

duplicate_count = (
    gold_count
    - gold_distinct_count
)

try:
    quarantine_count = quarantine_df.count()
except Exception:
    quarantine_count = 0

print("Bronze      :", bronze_count)
print("Silver      :", silver_count)
print("Gold        :", gold_count)
print("Gold unique :", gold_distinct_count)
print("Duplicates  :", duplicate_count)
print("Quarantine  :", quarantine_count)

# COMMAND ----------

# DBTITLE 1,Validate active model state
active_model_count = (
    spark.table(
        "fraud_dev.control.model_status"
    )
    .filter(
        (F.col("model_name") ==
         "fraud_dev.ml.fraud_detection_model")
        &
        (F.col("model_status") == "APPROVED")
        &
        (F.col("is_active") == True)
    )
    .count()
)

print(
    "Active approved models:",
    active_model_count
)

# COMMAND ----------

# DBTITLE 1,Determine reconciliation result
reconciliation_passed = (
    bronze_count == silver_count
    and
    silver_count == gold_count
    and
    gold_count == gold_distinct_count
    and
    duplicate_count == 0
    and
    quarantine_count == 0
    and
    active_model_count == 1
)

print(
    "RECONCILIATION:",
    "PASS"
    if reconciliation_passed
    else "FAIL"
)

# COMMAND ----------

# DBTITLE 1,Write the reconciliation audit record
failure_reasons = []

if bronze_count != silver_count:
    failure_reasons.append(
        f"BRONZE_SILVER_MISMATCH:{bronze_count}!={silver_count}"
    )

if silver_count != gold_count:
    failure_reasons.append(
        f"SILVER_GOLD_MISMATCH:{silver_count}!={gold_count}"
    )

if duplicate_count > 0:
    failure_reasons.append(
        f"GOLD_DUPLICATES:{duplicate_count}"
    )

if quarantine_count > 0:
    failure_reasons.append(
        f"QUARANTINE_ROWS:{quarantine_count}"
    )

if active_model_count != 1:
    failure_reasons.append(
        f"ACTIVE_MODEL_COUNT:{active_model_count}"
    )

failure_reason = (
    None
    if reconciliation_passed
    else " | ".join(failure_reasons)
)

# COMMAND ----------

# DBTITLE 1,Cell 7
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)

recon_df = spark.createDataFrame(
    [(
        RECON_RUN_ID,
        "transactions",
        "GOLD",
        int(bronze_count),
        int(gold_count),
        int(quarantine_count),
        int(duplicate_count),
        (
            "PASS"
            if reconciliation_passed
            else "FAIL"
        ),
        datetime.now(),
        failure_reason
    )],
    StructType([
        StructField("pipeline_run_id", StringType(), True),
        StructField("entity_name", StringType(), True),
        StructField("layer_name", StringType(), True),
        StructField("source_count", IntegerType(), True),
        StructField("target_count", IntegerType(), True),
        StructField("quarantine_count", IntegerType(), True),
        StructField("duplicate_count", IntegerType(), True),
        StructField("reconciliation_status", StringType(), True),
        StructField("reconciliation_timestamp", TimestampType(), True),
        StructField("failure_reason", StringType(), True)
    ])
)

(
    recon_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(
        "fraud_dev.control.reconciliation_results"
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM fraud_dev.control.reconciliation_results
# MAGIC ORDER BY reconciliation_timestamp DESC
# MAGIC LIMIT 10;

# COMMAND ----------

bronze_source_files = (
    bronze_df

    .select(
        F.regexp_extract(
            F.col("_source_file"),
            r"([^/]+$)",
            1
        ).alias("file_name")
    )

    .filter(
        F.col("file_name") != ""
    )

    .dropDuplicates(
        ["file_name"]
    )
)

bronze_source_files.show(
    truncate=False
)

# COMMAND ----------

archive_candidates_df = (
    spark.table(
        "fraud_dev.control.file_processing_status"
    )
    .alias("c")

    .join(
        bronze_source_files.alias("b"),
        F.col("c.file_name") == F.col("b.file_name"),
        "inner"
    )

    .filter(
        (F.col("c.entity_name") == "transactions")
        &
        (F.col("c.validation_status") == "PASS")
        &
        (
            F.col("c.processing_status").isin(
                "READY",
                "BRONZE_LOADED",
                "SILVER_LOADED",
                "PROCESSED"
            )
        )
    )

    .select(
        F.col("c.file_name").alias(
            "file_name"
        )
    )

    .dropDuplicates(
        ["file_name"]
    )
)

# COMMAND ----------

archive_candidates_df.show(
    truncate=False
)

print(
    "Files eligible:",
    archive_candidates_df.count()
)

# COMMAND ----------

# DBTITLE 1,Archive only if reconciliation passed
TRANSACTION_INCOMING = (
    "/Volumes/fraud_dev/staging/source_files/"
    "transactions/incoming/"
)

TRANSACTION_ARCHIVE = (
    "/Volumes/fraud_dev/staging/source_files/"
    "transactions/archive/"
)

# COMMAND ----------

archived_files = []
archive_failures = []

if reconciliation_passed:

    candidate_files = (
        archive_candidates_df.collect()
    )

    for row in candidate_files:

        file_name = row["file_name"]

        source_path = (
            TRANSACTION_INCOMING
            + file_name
        )

        destination_path = (
            TRANSACTION_ARCHIVE
            + file_name
        )

        try:

            dbutils.fs.mv(
                source_path,
                destination_path
            )

            archived_files.append(
                file_name
            )

            print(
                "ARCHIVED:",
                file_name
            )

        except Exception as e:

            archive_failures.append(
                (
                    file_name,
                    str(e)
                )
            )

            print(
                "ARCHIVE FAILED:",
                file_name,
                str(e)
            )

else:

    print(
        "Reconciliation FAILED. "
        "No source files will be archived."
    )

# COMMAND ----------

archive_candidate_count = archive_candidates_df.count()

print(
    "Archive candidates:",
    archive_candidate_count
)

print(
    "Archived files:",
    len(archived_files)
)

print(
    "Archive failures:",
    len(archive_failures)
)

# COMMAND ----------

# DBTITLE 1,Update control only for successfully archived files
from delta.tables import DeltaTable

control_delta = DeltaTable.forName(
    spark,
    "fraud_dev.control.file_processing_status"
)

# COMMAND ----------

for file_name in archived_files:

    control_delta.update(
        condition=(
            (F.col("entity_name") == "transactions")
            &
            (F.col("file_name") == file_name)
        ),
        set={
            "processing_status":
                F.lit("ARCHIVED"),

            "processed_at":
                F.current_timestamp(),

            "archived_at":
                F.current_timestamp()
        }
    )

# COMMAND ----------

# DBTITLE 1,Final pipeline status
END_TO_END_SUCCESS = (
    reconciliation_passed
    and
    len(archive_failures) == 0
    and
    len(archived_files)
        == archive_candidate_count
)

# COMMAND ----------

print(
    "END-TO-END STATUS:",
    (
        "SUCCESS"
        if END_TO_END_SUCCESS
        else "FAILED"
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 19
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)

pipeline_run_df = spark.createDataFrame(
    [(
        RECON_RUN_ID,
        "fraud_transaction_pipeline",
        (
            "SUCCESS"
            if END_TO_END_SUCCESS
            else "FAILED"
        ),
        datetime.now(),
        datetime.now(),
        (
            "GOLD"
            if END_TO_END_SUCCESS
            else None
        ),
        (
            None
            if END_TO_END_SUCCESS
            else (
                "ARCHIVE"
                if archive_failures
                else "RECONCILIATION"
            )
        ),
        (
            None
            if END_TO_END_SUCCESS
            else (
                str(archive_failures)
                if archive_failures
                else failure_reason
            )
        ),
        (
            "NOT_REQUIRED"
            if END_TO_END_SUCCESS
            else "PENDING"
        )
    )],
    StructType([
        StructField("pipeline_run_id", StringType(), True),
        StructField("pipeline_name", StringType(), True),
        StructField("run_status", StringType(), True),
        StructField("start_timestamp", TimestampType(), True),
        StructField("end_timestamp", TimestampType(), True),
        StructField("last_successful_layer", StringType(), True),
        StructField("failure_layer", StringType(), True),
        StructField("failure_reason", StringType(), True),
        StructField("recovery_status", StringType(), True)
    ])
)

(
    pipeline_run_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(
        "fraud_dev.control.pipeline_runs"
    )
)

# COMMAND ----------

# DBTITLE 1,Final verification
display(
    dbutils.fs.ls(
        TRANSACTION_INCOMING
    )
)

# COMMAND ----------

display(
    dbutils.fs.ls(
        TRANSACTION_ARCHIVE
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     file_name,
# MAGIC     validation_status,
# MAGIC     processing_status,
# MAGIC     archived_at
# MAGIC FROM fraud_dev.control.file_processing_status
# MAGIC WHERE entity_name = 'transactions'
# MAGIC ORDER BY file_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM fraud_dev.control.pipeline_runs
# MAGIC ORDER BY end_timestamp DESC
# MAGIC LIMIT 10;