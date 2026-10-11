# Databricks notebook source
# DBTITLE 1,Parameters
from pyspark.sql import functions as F
from delta.tables import DeltaTable

dbutils.widgets.text("ENTITY", "customers")
ENTITY = dbutils.widgets.get("ENTITY").strip().lower()

if ENTITY not in ["customers", "merchants"]:
    raise ValueError("ENTITY must be customers or merchants")

print("Processing:", ENTITY)

# COMMAND ----------

if ENTITY == "customers":

    source_df = spark.table("fraud_dev.bronze.customers")

    silver_df = (
        source_df

        .withColumn(
            "date_of_birth",
            F.to_date("date_of_birth")
        )

        .withColumn(
            "customer_since",
            F.to_date("customer_since")
        )

        .withColumn(
            "avg_monthly_spend",
            F.col("avg_monthly_spend").cast("double")
        )

        .withColumn(
            "avg_transaction_amount",
            F.col("avg_transaction_amount").cast("double")
        )

        .withColumn(
            "customer_status",
            F.upper(F.trim("customer_status"))
        )

        .withColumn(
            "customer_risk_level",
            F.upper(F.trim("customer_risk_level"))
        )

        .withColumn(
            "preferred_currency",
            F.upper(F.trim("preferred_currency"))
        )
    )

# COMMAND ----------

    valid_df = (
        silver_df

        .filter(
            F.col("customer_id").isNotNull()
        )

        .filter(
            F.col("customer_name").isNotNull()
        )

        .filter(
            F.col("avg_transaction_amount").isNull() |
            (F.col("avg_transaction_amount") >= 0)
        )

        .filter(
            F.col("avg_monthly_spend").isNull() |
            (F.col("avg_monthly_spend") >= 0)
        )

        .filter(
            F.col("customer_risk_level").isin(
                "LOW",
                "MEDIUM",
                "HIGH",
                "CRITICAL"
            )
        )

        .dropDuplicates(["customer_id"])
    )

# COMMAND ----------

    final_df = valid_df.select(
        "customer_id",
        "customer_name",
        "date_of_birth",
        "customer_since",
        "customer_status",
        "home_country",
        "home_state",
        "home_city",
        "preferred_currency",
        "customer_risk_level",
        "avg_monthly_spend",
        "avg_transaction_amount",
        "created_at",
        "updated_at",
        "_ingestion_timestamp",
        "_pipeline_run_id",
        "_load_type"
    )

# COMMAND ----------

    (
        final_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("fraud_dev.silver.customers")
    )

    print("Silver customers complete")

# COMMAND ----------

if ENTITY == "merchants":

    source_df = spark.table("fraud_dev.bronze.merchants")

    silver_df = (
        source_df

        .withColumn(
            "chargeback_rate",
            F.col("chargeback_rate").cast("double")
        )

        .withColumn(
            "fraud_rate",
            F.col("fraud_rate").cast("double")
        )

        .withColumn(
            "merchant_status",
            F.upper(F.trim("merchant_status"))
        )

        .withColumn(
            "merchant_risk_level",
            F.upper(F.trim("merchant_risk_level"))
        )

        .withColumn(
            "merchant_category",
            F.upper(F.trim("merchant_category"))
        )
    )

# COMMAND ----------

    valid_df = (
        silver_df

        .filter(
            F.col("merchant_id").isNotNull()
        )

        .filter(
            F.col("merchant_name").isNotNull()
        )

        .filter(
            F.col("merchant_category").isNotNull()
        )

        .filter(
            F.col("merchant_risk_level").isin(
                "LOW",
                "MEDIUM",
                "HIGH"
            )
        )

        .filter(
            F.col("chargeback_rate").isNull() |
            (
                (F.col("chargeback_rate") >= 0) &
                (F.col("chargeback_rate") <= 1)
            )
        )

        .filter(
            F.col("fraud_rate").isNull() |
            (
                (F.col("fraud_rate") >= 0) &
                (F.col("fraud_rate") <= 1)
            )
        )

        .dropDuplicates(["merchant_id"])
    )

# COMMAND ----------

    final_df = valid_df.select(
        "merchant_id",
        "merchant_name",
        "merchant_category",
        "merchant_country",
        "merchant_state",
        "merchant_city",
        "merchant_status",
        "merchant_risk_level",
        "chargeback_rate",
        "fraud_rate",
        "created_at",
        "updated_at",
        "_ingestion_timestamp",
        "_pipeline_run_id",
        "_load_type"
    )

# COMMAND ----------

    (
        final_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("fraud_dev.silver.merchants")
    )

    print("Silver merchants complete")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS customer_count
# MAGIC FROM fraud_dev.silver.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS merchant_count
# MAGIC FROM fraud_dev.silver.merchants;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, COUNT(*) AS cnt
# MAGIC FROM fraud_dev.silver.customers
# MAGIC GROUP BY customer_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT merchant_id, COUNT(*) AS cnt
# MAGIC FROM fraud_dev.silver.merchants
# MAGIC GROUP BY merchant_id
# MAGIC HAVING COUNT(*) > 1;