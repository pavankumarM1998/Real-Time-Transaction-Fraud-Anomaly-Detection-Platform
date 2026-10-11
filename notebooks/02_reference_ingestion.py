# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
import uuid

# COMMAND ----------

# DBTITLE 1,Create the widget
dbutils.widgets.text("ENTITY", "customers")

ENTITY = dbutils.widgets.get("ENTITY").strip().lower()

valid_entities = ["customers", "merchants", "fraud_labels"]

if ENTITY not in valid_entities:
    raise ValueError(
        f"Unsupported ENTITY: {ENTITY}. "
        f"Valid values: {valid_entities}"
    )

print("Processing reference entity:", ENTITY)

# COMMAND ----------

BASE_PATH = "/Volumes/fraud_dev/staging/source_files"
SOURCE_PATH = f"{BASE_PATH}/{ENTITY}/incoming"
PIPELINE_RUN_ID = str(uuid.uuid4())

print("Source:", SOURCE_PATH)
print("Run ID:", PIPELINE_RUN_ID)

# COMMAND ----------

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("customer_since", StringType(), True),
    StructField("customer_status", StringType(), True),
    StructField("home_country", StringType(), True),
    StructField("home_state", StringType(), True),
    StructField("home_city", StringType(), True),
    StructField("preferred_currency", StringType(), True),
    StructField("customer_risk_level", StringType(), True),
    StructField("avg_monthly_spend", StringType(), True),
    StructField("avg_transaction_amount", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True)
])

# COMMAND ----------

MERCHANT_SCHEMA = StructType([
    StructField("merchant_id", StringType(), False),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("merchant_country", StringType(), True),
    StructField("merchant_state", StringType(), True),
    StructField("merchant_city", StringType(), True),
    StructField("merchant_status", StringType(), True),
    StructField("merchant_risk_level", StringType(), True),
    StructField("chargeback_rate", StringType(), True),
    StructField("fraud_rate", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True)
])

# COMMAND ----------

FRAUD_LABEL_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("transaction_timestamp", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("currency_code", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("velocity_5m", StringType(), True),
    StructField("velocity_30m", StringType(), True),
    StructField("amount_vs_customer_avg", StringType(), True),
    StructField("merchant_risk_score", StringType(), True),
    StructField("is_fraud", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Map entity to schema
SCHEMA_MAP = {
    "customers": CUSTOMER_SCHEMA,
    "merchants": MERCHANT_SCHEMA,
    "fraud_labels": FRAUD_LABEL_SCHEMA
}

schema = SCHEMA_MAP[ENTITY]

# COMMAND ----------

ready_files_df = (
    spark.table("fraud_dev.control.file_processing_status")
    .filter(
        (F.col("entity_name") == ENTITY) &
        (F.col("validation_status") == "PASS") &
        (F.col("processing_status") == "READY")
    )
    .select("file_path")
    .distinct()
)

ready_files = [
    r["file_path"]
    for r in ready_files_df.collect()
]

print("Ready files:", len(ready_files))
for f in ready_files:
    print(f)

# COMMAND ----------

source_df = (
    spark.read
    .schema(schema)
    .option("header", "true")
    .csv(ready_files)
)

# COMMAND ----------

bronze_df = (
    source_df
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_pipeline_run_id", F.lit(PIPELINE_RUN_ID))
    .withColumn("_load_type", F.lit("INITIAL"))
)

# COMMAND ----------

target_table = f"fraud_dev.bronze.{ENTITY}"

(
    bronze_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(target_table)
)

print("Written to:", target_table)

# COMMAND ----------

loaded_count = bronze_df.count()

print("Rows loaded:", loaded_count)

# COMMAND ----------

from delta.tables import DeltaTable

control_table = DeltaTable.forName(
    spark,
    "fraud_dev.control.file_processing_status"
)

processed_files_df = spark.createDataFrame(
    [(f,) for f in ready_files],
    ["file_path"]
)

(
    control_table.alias("t")
    .merge(
        processed_files_df.alias("s"),
        "t.file_path = s.file_path"
    )
    .whenMatchedUpdate(
        condition="t.processing_status = 'READY'",
        set={
            "processing_status": F.lit("PROCESSED"),
            "processed_at": F.current_timestamp()
        }
    )
    .execute()
)