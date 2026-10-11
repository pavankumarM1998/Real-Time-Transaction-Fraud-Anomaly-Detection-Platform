# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
import uuid

# COMMAND ----------

SOURCE_PATH = (
    "/Volumes/fraud_dev/staging/source_files/"
    "transactions/incoming/"
)

CHECKPOINT_PATH = (
    "/Volumes/fraud_dev/staging/source_files/"
    "checkpoints/bronze_transactions/"
)

SCHEMA_PATH = (
    "/Volumes/fraud_dev/staging/source_files/"
    "checkpoints/bronze_transactions_schema/"
)

TARGET_TABLE = "fraud_dev.bronze.transactions"

PIPELINE_RUN_ID = str(uuid.uuid4())

print("Source Path      :", SOURCE_PATH)
print("Checkpoint Path  :", CHECKPOINT_PATH)
print("Target Table     :", TARGET_TABLE)
print("Pipeline Run ID  :", PIPELINE_RUN_ID)

# COMMAND ----------

# DBTITLE 1,Explicit transaction schema
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("transaction_timestamp", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("currency_code", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("transaction_status", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("country_code", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Create the Auto Loader stream
raw_stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    .option("cloudFiles.schemaEvolutionMode", "none")
    .schema(TRANSACTION_SCHEMA)
    .load(SOURCE_PATH)
)

# COMMAND ----------

# DBTITLE 1,Add Bronze lineage
bronze_stream_df = (
    raw_stream_df

    .withColumn(
        "_ingestion_timestamp",
        F.current_timestamp()
    )

    .withColumn(
        "_source_file",
        F.col("_metadata.file_path")
    )

    .withColumn(
        "_pipeline_run_id",
        F.lit(PIPELINE_RUN_ID)
    )

    .withColumn(
        "_load_type",
        F.when(
            F.col("_metadata.file_name")
            .startswith("transactions_initial_"),
            F.lit("INITIAL")
        ).otherwise(
            F.lit("INCREMENTAL")
        )
    )
)

# COMMAND ----------

# DBTITLE 1,Preview the schema
bronze_stream_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Write with AvailableNow
query = (
    bronze_stream_df.writeStream

    .format("delta")

    .option(
        "checkpointLocation",
        CHECKPOINT_PATH
    )

    .outputMode("append")

    .trigger(availableNow=True)

    .toTable(TARGET_TABLE)
)

query.awaitTermination()

print("Bronze transaction stream completed.")

# COMMAND ----------

# DBTITLE 1,Check what this run loaded
current_run_df = (
    spark.table(TARGET_TABLE)
    .filter(
        F.col("_pipeline_run_id") == PIPELINE_RUN_ID
    )
)

current_run_count = current_run_df.count()

print(
    "Rows loaded in this run:",
    current_run_count
)

# COMMAND ----------

# DBTITLE 1,Verify files processed in this run
display(
    current_run_df
    .groupBy(
        "_source_file",
        "_load_type"
    )
    .agg(
        F.count("*").alias("rows_loaded")
    )
    .orderBy("_source_file")
)

# COMMAND ----------

# DBTITLE 1,Check total Bronze count
total_bronze_count = (
    spark.table(TARGET_TABLE)
    .count()
)

print(
    "Total Bronze transaction rows:",
    total_bronze_count
)

# COMMAND ----------

# DBTITLE 1,Check duplicates in the initial source
duplicate_count = (
    spark.table(TARGET_TABLE)
    .groupBy("transaction_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    "Duplicate transaction IDs currently in Bronze:",
    duplicate_count
)

# COMMAND ----------

# DBTITLE 1,Verify load type
# MAGIC %sql
# MAGIC SELECT
# MAGIC     _load_type,
# MAGIC     COUNT(*) AS row_count
# MAGIC FROM fraud_dev.bronze.transactions
# MAGIC GROUP BY _load_type;

# COMMAND ----------

# DBTITLE 1,Verify pipeline-run lineage
# MAGIC %sql
# MAGIC SELECT
# MAGIC     _pipeline_run_id,
# MAGIC     _load_type,
# MAGIC     COUNT(*) AS rows_loaded,
# MAGIC     COUNT(DISTINCT _source_file) AS files_loaded,
# MAGIC     MIN(_ingestion_timestamp) AS ingestion_started,
# MAGIC     MAX(_ingestion_timestamp) AS ingestion_finished
# MAGIC FROM fraud_dev.bronze.transactions
# MAGIC GROUP BY
# MAGIC     _pipeline_run_id,
# MAGIC     _load_type
# MAGIC ORDER BY ingestion_started DESC;

# COMMAND ----------

loaded_files = [
    r["_source_file"]
    for r in (
        current_run_df
        .select("_source_file")
        .distinct()
        .collect()
    )
]

print("Files loaded:", len(loaded_files))

# COMMAND ----------

from delta.tables import DeltaTable

if loaded_files:

    loaded_files_df = spark.createDataFrame(
        [(x,) for x in loaded_files],
        ["file_path"]
    )

    control_delta = DeltaTable.forName(
        spark,
        "fraud_dev.control.file_processing_status"
    )

    (
        control_delta.alias("t")
        .merge(
            loaded_files_df.alias("s"),
            "t.file_path = s.file_path"
        )
        .whenMatchedUpdate(
            condition="""
                t.entity_name = 'transactions'
                AND t.validation_status = 'PASS'
            """,
            set={
                "processing_status":
                    F.lit("BRONZE_LOADED")
            }
        )
        .execute()
    )

# COMMAND ----------

# DBTITLE 1,Verify control state
# MAGIC %sql
# MAGIC SELECT
# MAGIC     file_name,
# MAGIC     validation_status,
# MAGIC     processing_status,
# MAGIC     row_count,
# MAGIC     pipeline_run_id
# MAGIC FROM fraud_dev.control.file_processing_status
# MAGIC WHERE entity_name = 'transactions'
# MAGIC ORDER BY file_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS bronze_rows
# MAGIC FROM fraud_dev.bronze.transactions;