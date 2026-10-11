# Databricks notebook source
from pyspark.sql import functions as F

txn_df = spark.table(
    "fraud_dev.silver.transaction_enriched_features"
)

velocity_5m_df = spark.table(
    "fraud_dev.silver.customer_velocity_5m"
)

velocity_10m_df = spark.table(
    "fraud_dev.silver.customer_velocity_10m"
)

# COMMAND ----------

print("Transactions :", txn_df.count())
print("5M windows   :", velocity_5m_df.count())
print("10M windows  :", velocity_10m_df.count())

# COMMAND ----------

# DBTITLE 1,Match transactions to their 5-minute windows
txn_5m_matches = (
    txn_df.alias("t")
    .join(
        velocity_5m_df.alias("v"),
        (
            (F.col("t.customer_id") == F.col("v.customer_id"))
            &
            (
                F.col("t.transaction_timestamp")
                >= F.col("v.window_5m_start")
            )
            &
            (
                F.col("t.transaction_timestamp")
                < F.col("v.window_5m_end")
            )
        ),
        "left"
    )
)

# COMMAND ----------

# DBTITLE 1,Because windows overlap, collapse them back to one record per transaction
txn_5m_features = (
    txn_5m_matches
    .groupBy("t.transaction_id")
    .agg(
        F.max("v.txn_count_5m").alias("txn_count_5m"),
        F.max("v.txn_amount_5m").alias("txn_amount_5m"),
        F.max("v.failed_txn_count_5m").alias(
            "failed_txn_count_5m"
        ),
        F.max("v.rule_velocity_5m").alias(
            "rule_velocity_5m"
        )
    )
)

# COMMAND ----------

# DBTITLE 1,Now create the 10-minute transaction-level features
txn_10m_matches = (
    txn_df.alias("t")
    .join(
        velocity_10m_df.alias("v"),
        (
            (F.col("t.customer_id") == F.col("v.customer_id"))
            &
            (
                F.col("t.transaction_timestamp")
                >= F.col("v.window_10m_start")
            )
            &
            (
                F.col("t.transaction_timestamp")
                < F.col("v.window_10m_end")
            )
        ),
        "left"
    )
)

# COMMAND ----------

# DBTITLE 1,Collapse again
txn_10m_features = (
    txn_10m_matches
    .groupBy("t.transaction_id")
    .agg(
        F.max("v.txn_count_10m").alias("txn_count_10m"),
        F.max("v.txn_amount_10m").alias("txn_amount_10m"),
        F.max("v.failed_txn_count_10m").alias(
            "failed_txn_count_10m"
        ),
        F.max("v.rule_high_value_10m").alias(
            "rule_high_value_10m"
        ),
        F.max("v.rule_repeated_failures").alias(
            "rule_repeated_failures"
        )
    )
)

# COMMAND ----------

# DBTITLE 1,Now join those transaction-level velocity features back to the enriched transactions
final_features_df = (
    txn_df

    .join(
        txn_5m_features,
        on="transaction_id",
        how="left"
    )

    .join(
        txn_10m_features,
        on="transaction_id",
        how="left"
    )
)

# COMMAND ----------

# DBTITLE 1,For transactions with no completed matching window yet, use zero values
final_features_df = (
    final_features_df

    .fillna({
        "txn_count_5m": 0,
        "txn_amount_5m": 0.0,
        "failed_txn_count_5m": 0,
        "rule_velocity_5m": 0,

        "txn_count_10m": 0,
        "txn_amount_10m": 0.0,
        "failed_txn_count_10m": 0,
        "rule_high_value_10m": 0,
        "rule_repeated_failures": 0
    })
)

# COMMAND ----------

WEIGHT_HIGH_AMOUNT = 20
WEIGHT_AMOUNT_ANOMALY = 25
WEIGHT_VELOCITY = 20
WEIGHT_HIGH_VALUE_10M = 15
WEIGHT_REPEATED_FAILURES = 10
WEIGHT_HIGH_RISK_MERCHANT = 10
WEIGHT_COUNTRY_ANOMALY = 5

# COMMAND ----------

final_features_df = (
    final_features_df

    .withColumn(
        "rule_score_raw",

        F.col("rule_high_amount")
        * WEIGHT_HIGH_AMOUNT

        +

        F.col("rule_amount_anomaly")
        * WEIGHT_AMOUNT_ANOMALY

        +

        F.col("rule_velocity_5m")
        * WEIGHT_VELOCITY

        +

        F.col("rule_high_value_10m")
        * WEIGHT_HIGH_VALUE_10M

        +

        F.col("rule_repeated_failures")
        * WEIGHT_REPEATED_FAILURES

        +

        F.col("rule_high_risk_merchant")
        * WEIGHT_HIGH_RISK_MERCHANT

        +

        F.col("rule_country_anomaly")
        * WEIGHT_COUNTRY_ANOMALY
    )
)

# COMMAND ----------

display(final_features_df)

# COMMAND ----------

# DBTITLE 1,Cap it at 100
final_features_df = (
    final_features_df
    .withColumn(
        "rule_score",
        F.least(
            F.col("rule_score_raw"),
            F.lit(100)
        )
    )
    .drop("rule_score_raw")
)

# COMMAND ----------

final_features_df = (
    final_features_df

    .withColumn(
        "rule_risk_level",

        F.when(
            F.col("rule_score") >= 80,
            "CRITICAL"
        )

        .when(
            F.col("rule_score") >= 60,
            "HIGH"
        )

        .when(
            F.col("rule_score") >= 30,
            "MEDIUM"
        )

        .otherwise("LOW")
    )
)

# COMMAND ----------

TARGET_TABLE = (
    "fraud_dev.silver.transaction_complete_fraud_features"
)


(
    final_features_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_rows
# MAGIC FROM fraud_dev.silver.transaction_complete_fraud_features;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     transaction_id,
# MAGIC     COUNT(*) AS cnt
# MAGIC FROM fraud_dev.silver.transaction_complete_fraud_features
# MAGIC GROUP BY transaction_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     rule_risk_level,
# MAGIC     COUNT(*) AS transactions
# MAGIC FROM fraud_dev.silver.transaction_complete_fraud_features
# MAGIC GROUP BY rule_risk_level
# MAGIC ORDER BY transactions DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     transaction_id,
# MAGIC     customer_id,
# MAGIC     merchant_id,
# MAGIC     amount,
# MAGIC
# MAGIC     amount_vs_customer_avg,
# MAGIC
# MAGIC     txn_count_5m,
# MAGIC     txn_amount_5m,
# MAGIC
# MAGIC     txn_count_10m,
# MAGIC     txn_amount_10m,
# MAGIC     failed_txn_count_10m,
# MAGIC
# MAGIC     rule_high_amount,
# MAGIC     rule_amount_anomaly,
# MAGIC     rule_velocity_5m,
# MAGIC     rule_high_value_10m,
# MAGIC     rule_repeated_failures,
# MAGIC     rule_high_risk_merchant,
# MAGIC     rule_country_anomaly,
# MAGIC
# MAGIC     rule_score,
# MAGIC     rule_risk_level
# MAGIC
# MAGIC FROM fraud_dev.silver.transaction_complete_fraud_features
# MAGIC
# MAGIC ORDER BY rule_score DESC
# MAGIC
# MAGIC LIMIT 50;