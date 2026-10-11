# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Read Silver as stream
SOURCE_TABLE = "fraud_dev.silver.transactions"

CHECKPOINT_PATH = (
    "/Volumes/fraud_dev/staging/source_files/"
    "checkpoints/fraud_processing/"
)

RULE_OUTPUT_TABLE = (
    "fraud_dev.silver.transaction_fraud_features"
)

# COMMAND ----------

transaction_stream = (
    spark.readStream
    .table(SOURCE_TABLE)

    .withWatermark(
        "transaction_timestamp",
        "30 minutes"
    )
)

# COMMAND ----------

# DBTITLE 1,Deduplicate across streaming runs
transaction_stream = (
    transaction_stream
    .dropDuplicatesWithinWatermark(
        ["transaction_id"]
    )
)

# COMMAND ----------

# DBTITLE 1,High amount rule
HIGH_AMOUNT_THRESHOLD = 50000.0

# COMMAND ----------

rule_df = (
    transaction_stream

    .withColumn(
        "rule_high_amount",
        F.when(
            F.col("amount") >= HIGH_AMOUNT_THRESHOLD,
            F.lit(1)
        ).otherwise(F.lit(0))
    )
)

# COMMAND ----------

# DBTITLE 1,5-minute transaction velocity
velocity_5m_df = (
    transaction_stream

    .groupBy(
        F.col("customer_id"),

        F.window(
            F.col("transaction_timestamp"),
            "5 minutes",
            "1 minute"
        )
    )

    .agg(
        F.count("*").alias("txn_count_5m"),

        F.sum("amount").alias(
            "txn_amount_5m"
        ),

        F.sum(
            F.when(
                F.col("transaction_status").isin(
                    "FAILED",
                    "DECLINED"
                ),
                1
            ).otherwise(0)
        ).alias("failed_txn_count_5m")
    )
)

# COMMAND ----------

# DBTITLE 1,10-minute velocity
velocity_10m_df = (
    transaction_stream

    .groupBy(
        F.col("customer_id"),

        F.window(
            F.col("transaction_timestamp"),
            "10 minutes",
            "1 minute"
        )
    )

    .agg(
        F.count("*").alias("txn_count_10m"),

        F.sum("amount").alias(
            "txn_amount_10m"
        ),

        F.sum(
            F.when(
                F.col("transaction_status").isin(
                    "FAILED",
                    "DECLINED"
                ),
                1
            ).otherwise(0)
        ).alias("failed_txn_count_10m")
    )
)

# COMMAND ----------

# DBTITLE 1,Initial thresholds
VELOCITY_5M_COUNT_THRESHOLD = 5
VELOCITY_10M_AMOUNT_THRESHOLD = 100000.0
FAILED_10M_THRESHOLD = 3

# COMMAND ----------

# DBTITLE 1,Convert aggregated features into rule signals
velocity_5m_rules = (
    velocity_5m_df

    .withColumn(
        "rule_velocity_5m",
        F.when(
            F.col("txn_count_5m")
            >= VELOCITY_5M_COUNT_THRESHOLD,
            1
        ).otherwise(0)
    )

    .select(
        "customer_id",
        F.col("window.start").alias(
            "window_5m_start"
        ),
        F.col("window.end").alias(
            "window_5m_end"
        ),
        "txn_count_5m",
        "txn_amount_5m",
        "failed_txn_count_5m",
        "rule_velocity_5m"
    )
)

# COMMAND ----------

velocity_10m_rules = (
    velocity_10m_df

    .withColumn(
        "rule_high_value_10m",
        F.when(
            F.col("txn_amount_10m")
            >= VELOCITY_10M_AMOUNT_THRESHOLD,
            1
        ).otherwise(0)
    )

    .withColumn(
        "rule_repeated_failures",
        F.when(
            F.col("failed_txn_count_10m")
            >= FAILED_10M_THRESHOLD,
            1
        ).otherwise(0)
    )

    .select(
        "customer_id",
        F.col("window.start").alias(
            "window_10m_start"
        ),
        F.col("window.end").alias(
            "window_10m_end"
        ),
        "txn_count_10m",
        "txn_amount_10m",
        "failed_txn_count_10m",
        "rule_high_value_10m",
        "rule_repeated_failures"
    )
)

# COMMAND ----------

# DBTITLE 1,Write 5-minute features
CHECKPOINT_5M = (
    "/Volumes/fraud_dev/staging/source_files/"
    "checkpoints/fraud_velocity_5m/"
)

# COMMAND ----------

query_5m = (
    velocity_5m_rules.writeStream

    .format("delta")

    .outputMode("append")

    .option(
        "checkpointLocation",
        CHECKPOINT_5M
    )

    .trigger(
        availableNow=True
    )

    .toTable(
        "fraud_dev.silver.customer_velocity_5m"
    )
)

query_5m.awaitTermination()

# COMMAND ----------

# DBTITLE 1,Transaction-level rule table
CHECKPOINT_RULES = (
    "/Volumes/fraud_dev/staging/source_files/"
    "checkpoints/fraud_transaction_rules/"
)

# COMMAND ----------

transaction_rules_df = (
    rule_df

    .select(
        "transaction_id",
        "customer_id",
        "merchant_id",
        "transaction_timestamp",
        "amount",
        "currency_code",
        "transaction_status",

        "rule_high_amount",

        "_pipeline_run_id",
        "_source_file",
        "_ingestion_timestamp",
        "_load_type"
    )
)

# COMMAND ----------

query_10m = (
    velocity_10m_rules.writeStream

    .format("delta")

    .outputMode("append")

    .option(
        "checkpointLocation",
        "/Volumes/fraud_dev/staging/source_files/"
    "checkpoints/fraud_velocity_10m/"
    )

    .trigger(availableNow=True)

    .toTable("fraud_dev.silver.customer_velocity_10m")
)

query_10m.awaitTermination()

print("10-minute fraud feature stream completed.")

# COMMAND ----------

query_rules = (
    transaction_rules_df.writeStream

    .format("delta")

    .outputMode("append")

    .option(
        "checkpointLocation",
        CHECKPOINT_RULES
    )

    .trigger(
        availableNow=True
    )

    .toTable(
        "fraud_dev.silver.transaction_fraud_features"
    )
)

query_rules.awaitTermination()

# COMMAND ----------

# DBTITLE 1,Verify rule output
# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS transaction_rule_rows
# MAGIC FROM fraud_dev.silver.transaction_fraud_features;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     rule_high_amount,
# MAGIC     COUNT(*) AS transactions
# MAGIC FROM fraud_dev.silver.transaction_fraud_features
# MAGIC GROUP BY rule_high_amount;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM fraud_dev.silver.customer_velocity_5m
# MAGIC WHERE rule_velocity_5m = 1
# MAGIC ORDER BY txn_count_5m DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM fraud_dev.silver.customer_velocity_10m
# MAGIC WHERE rule_repeated_failures = 1
# MAGIC ORDER BY failed_txn_count_10m DESC
# MAGIC LIMIT 20;