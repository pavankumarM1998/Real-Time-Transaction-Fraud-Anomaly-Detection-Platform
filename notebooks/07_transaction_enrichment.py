# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Load the three trusted Silver tables
transactions_df = spark.table(
    "fraud_dev.silver.transactions"
)

customers_df = (
    spark.table("fraud_dev.silver.customers")
    .select(
        "customer_id",
        "home_country",
        "customer_risk_level",
        "avg_monthly_spend",
        "avg_transaction_amount"
    )
)

merchants_df = (
    spark.table("fraud_dev.silver.merchants")
    .select(
        "merchant_id",
        "merchant_risk_level",
        "fraud_rate",
        "chargeback_rate"
    )
)

# COMMAND ----------

enriched_df = (
    transactions_df.alias("t")

    .join(
        customers_df.alias("c"),
        F.col("t.customer_id")
        == F.col("c.customer_id"),
        "left"
    )

    .join(
        merchants_df.alias("m"),
        F.col("t.merchant_id")
        == F.col("m.merchant_id"),
        "left"
    )
)

# COMMAND ----------

enriched_df = enriched_df.select(

    F.col("t.transaction_id"),
    F.col("t.customer_id"),
    F.col("t.merchant_id"),
    F.col("t.transaction_timestamp"),
    F.col("t.amount"),
    F.col("t.currency_code"),
    F.col("t.payment_method"),
    F.col("t.merchant_category"),
    F.col("t.transaction_status"),
    F.col("t.device_id"),
    F.col("t.ip_address"),
    F.col("t.latitude"),
    F.col("t.longitude"),
    F.col("t.country_code"),

    F.col("c.home_country"),
    F.col("c.customer_risk_level"),
    F.col("c.avg_monthly_spend"),
    F.col("c.avg_transaction_amount"),

    F.col("m.merchant_risk_level"),
    F.col("m.fraud_rate"),
    F.col("m.chargeback_rate"),

    F.col("t._ingestion_timestamp"),
    F.col("t._source_file"),
    F.col("t._pipeline_run_id"),
    F.col("t._load_type")
)

# COMMAND ----------

# DBTITLE 1,Now calculate the customer amount-anomaly ratio
enriched_df = (
    enriched_df

    .withColumn(
        "amount_vs_customer_avg",
        F.when(
            F.col("avg_transaction_amount") > 0,
            F.col("amount")
            / F.col("avg_transaction_amount")
        ).otherwise(None)
    )
)

# COMMAND ----------

# DBTITLE 1,Now add the customer amount-anomaly rule
AMOUNT_ANOMALY_RATIO_THRESHOLD = 5.0

enriched_df = (
    enriched_df

    .withColumn(
        "rule_amount_anomaly",
        F.when(
            F.col("amount_vs_customer_avg")
            >= AMOUNT_ANOMALY_RATIO_THRESHOLD,
            1
        ).otherwise(0)
    )
)

# COMMAND ----------

# DBTITLE 1,Now add the merchant-risk rule
enriched_df = (
    enriched_df

    .withColumn(
        "rule_high_risk_merchant",
        F.when(
            F.col("merchant_risk_level") == "HIGH",
            1
        ).otherwise(0)
    )
)

# COMMAND ----------

# DBTITLE 1,Now add country anomaly
enriched_df = (
    enriched_df

    .withColumn(
        "rule_country_anomaly",
        F.when(
            F.col("home_country").isNotNull()
            & F.col("country_code").isNotNull()
            & (
                F.upper(F.col("home_country"))
                != F.upper(F.col("country_code"))
            ),
            1
        ).otherwise(0)
    )
)

# COMMAND ----------

base_rules_df = (
    spark.table(
        "fraud_dev.silver.transaction_fraud_features"
    )
    .select(
        "transaction_id",
        "rule_high_amount"
    )
    .dropDuplicates(["transaction_id"])
)

# COMMAND ----------

enriched_df = (
    enriched_df

    .join(
        base_rules_df,
        on="transaction_id",
        how="left"
    )

    .fillna(
        {"rule_high_amount": 0}
    )
)

# COMMAND ----------

enriched_df = (
    enriched_df

    .withColumn(
        "rule_score_base",

        F.col("rule_high_amount") * 20
        +
        F.col("rule_amount_anomaly") * 25
        +
        F.col("rule_high_risk_merchant") * 15
        +
        F.col("rule_country_anomaly") * 10
    )
)

# COMMAND ----------

TARGET_TABLE = (
    "fraud_dev.silver.transaction_enriched_features"
)
(
    enriched_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rows
# MAGIC FROM fraud_dev.silver.transaction_enriched_features;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS missing_customer_reference
# MAGIC FROM fraud_dev.silver.transaction_enriched_features
# MAGIC WHERE customer_risk_level IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS missing_merchant_reference
# MAGIC FROM fraud_dev.silver.transaction_enriched_features
# MAGIC WHERE merchant_risk_level IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     rule_amount_anomaly,
# MAGIC     COUNT(*) AS rows
# MAGIC FROM fraud_dev.silver.transaction_enriched_features
# MAGIC GROUP BY rule_amount_anomaly;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     rule_high_risk_merchant,
# MAGIC     COUNT(*) AS rows
# MAGIC FROM fraud_dev.silver.transaction_enriched_features
# MAGIC GROUP BY rule_high_risk_merchant;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     rule_country_anomaly,
# MAGIC     COUNT(*) AS rows
# MAGIC FROM fraud_dev.silver.transaction_enriched_features
# MAGIC GROUP BY rule_country_anomaly;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     transaction_id,
# MAGIC     customer_id,
# MAGIC     merchant_id,
# MAGIC     amount,
# MAGIC     avg_transaction_amount,
# MAGIC     amount_vs_customer_avg,
# MAGIC     customer_risk_level,
# MAGIC     merchant_risk_level,
# MAGIC     fraud_rate,
# MAGIC     chargeback_rate,
# MAGIC     rule_high_amount,
# MAGIC     rule_amount_anomaly,
# MAGIC     rule_high_risk_merchant,
# MAGIC     rule_country_anomaly,
# MAGIC     rule_score_base
# MAGIC FROM fraud_dev.silver.transaction_enriched_features
# MAGIC ORDER BY rule_score_base DESC
# MAGIC LIMIT 25;