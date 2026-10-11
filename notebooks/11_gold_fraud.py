# Databricks notebook source
# DBTITLE 1,Load trusted scored transactions
SIMULATE_GOLD_FAILURE = False

from pyspark.sql import functions as F

source_df = spark.table(
    "fraud_dev.silver.transaction_fraud_scored"
)

print(
    "Silver scored rows:",
    source_df.count()
)

print(
    "Distinct transactions:",
    source_df.select(
        "transaction_id"
    ).distinct().count()
)

# ==================================================
# FAILURE TEST 6 — SIMULATED GOLD FAILURE
# ==================================================

if SIMULATE_GOLD_FAILURE:

    raise Exception(
        "SIMULATED_GOLD_WRITE_FAILURE"
    )

# COMMAND ----------

# DBTITLE 1,Gold transaction fraud decisions
gold_transaction_df = (
    source_df

    .select(
        "transaction_id",
        "customer_id",
        "merchant_id",
        "transaction_timestamp",
        "amount",
        "currency_code",
        "payment_method",
        "merchant_category",
        "transaction_status",
        "country_code",

        F.round(
            "ml_fraud_probability",
            6
        ).alias(
            "fraud_probability"
        ),

        F.round(
            "rule_score",
            2
        ).alias(
            "rule_score"
        ),

        F.round(
            "final_fraud_score",
            2
        ).alias(
            "fraud_score"
        ),

        F.col(
            "final_fraud_prediction"
        ).alias(
            "is_suspected_fraud"
        ),

        F.col(
            "final_risk_level"
        ).alias(
            "risk_level"
        )
    )
)

# COMMAND ----------

(
    gold_transaction_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "fraud_dev.gold.fraud_transaction_decisions"
    )
)

# COMMAND ----------

# DBTITLE 1,Customer fraud summary
customer_summary_df = (
    gold_transaction_df

    .groupBy(
        "customer_id"
    )

    .agg(
        F.count("*").alias(
            "total_transactions"
        ),

        F.round(
            F.sum("amount"),
            2
        ).alias(
            "total_transaction_amount"
        ),

        F.sum(
            "is_suspected_fraud"
        ).alias(
            "suspected_fraud_transactions"
        ),

        F.round(
            F.sum(
                F.when(
                    F.col(
                        "is_suspected_fraud"
                    ) == 1,
                    F.col("amount")
                ).otherwise(0.0)
            ),
            2
        ).alias(
            "suspected_fraud_amount"
        ),

        F.round(
            F.avg(
                "fraud_score"
            ),
            2
        ).alias(
            "average_fraud_score"
        ),

        F.round(
            F.max(
                "fraud_score"
            ),
            2
        ).alias(
            "maximum_fraud_score"
        ),

        F.max(
            "transaction_timestamp"
        ).alias(
            "latest_transaction_timestamp"
        )
    )
)

# COMMAND ----------

customer_summary_df = (
    customer_summary_df

    .withColumn(
        "suspected_fraud_percentage",

        F.round(
            (
                F.col(
                    "suspected_fraud_transactions"
                )
                /
                F.col(
                    "total_transactions"
                )
            ) * 100,
            2
        )
    )
)

# COMMAND ----------

customer_risk_df = (
    gold_transaction_df

    .withColumn(
        "_risk_rank",

        F.when(
            F.col("risk_level") == "CRITICAL",
            4
        )

        .when(
            F.col("risk_level") == "HIGH",
            3
        )

        .when(
            F.col("risk_level") == "MEDIUM",
            2
        )

        .otherwise(1)
    )

    .groupBy(
        "customer_id"
    )

    .agg(
        F.max(
            "_risk_rank"
        ).alias(
            "_max_risk_rank"
        )
    )

    .withColumn(
        "highest_risk_level",

        F.when(
            F.col(
                "_max_risk_rank"
            ) == 4,
            "CRITICAL"
        )

        .when(
            F.col(
                "_max_risk_rank"
            ) == 3,
            "HIGH"
        )

        .when(
            F.col(
                "_max_risk_rank"
            ) == 2,
            "MEDIUM"
        )

        .otherwise("LOW")
    )

    .select(
        "customer_id",
        "highest_risk_level"
    )
)

# COMMAND ----------

customer_summary_df = (
    customer_summary_df

    .join(
        customer_risk_df,
        on="customer_id",
        how="left"
    )
)

# COMMAND ----------

(
    customer_summary_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "fraud_dev.gold.customer_fraud_summary"
    )
)

# COMMAND ----------

# DBTITLE 1,Merchant fraud summary
merchant_summary_df = (
    gold_transaction_df

    .groupBy(
        "merchant_id",
        "merchant_category"
    )

    .agg(
        F.count("*").alias(
            "total_transactions"
        ),

        F.round(
            F.sum("amount"),
            2
        ).alias(
            "total_transaction_amount"
        ),

        F.sum(
            "is_suspected_fraud"
        ).alias(
            "suspected_fraud_transactions"
        ),

        F.round(
            F.sum(
                F.when(
                    F.col(
                        "is_suspected_fraud"
                    ) == 1,
                    F.col("amount")
                ).otherwise(0.0)
            ),
            2
        ).alias(
            "suspected_fraud_amount"
        ),

        F.round(
            F.avg(
                "fraud_score"
            ),
            2
        ).alias(
            "average_fraud_score"
        ),

        F.round(
            F.max(
                "fraud_score"
            ),
            2
        ).alias(
            "maximum_fraud_score"
        )
    )
)

# COMMAND ----------

merchant_summary_df = (
    merchant_summary_df

    .withColumn(
        "suspected_fraud_percentage",

        F.round(
            (
                F.col(
                    "suspected_fraud_transactions"
                )
                /
                F.col(
                    "total_transactions"
                )
            ) * 100,
            2
        )
    )
)

# COMMAND ----------

(
    merchant_summary_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "fraud_dev.gold.merchant_fraud_summary"
    )
)

# COMMAND ----------

# DBTITLE 1,Gold validation
# MAGIC %sql
# MAGIC SELECT COUNT(*) AS gold_transactions
# MAGIC FROM fraud_dev.gold.fraud_transaction_decisions;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     transaction_id,
# MAGIC     COUNT(*) AS cnt
# MAGIC FROM fraud_dev.gold.fraud_transaction_decisions
# MAGIC GROUP BY transaction_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     risk_level,
# MAGIC     COUNT(*) AS transactions,
# MAGIC     ROUND(SUM(amount), 2) AS transaction_amount
# MAGIC FROM fraud_dev.gold.fraud_transaction_decisions
# MAGIC GROUP BY risk_level
# MAGIC ORDER BY
# MAGIC     CASE risk_level
# MAGIC         WHEN 'CRITICAL' THEN 1
# MAGIC         WHEN 'HIGH' THEN 2
# MAGIC         WHEN 'MEDIUM' THEN 3
# MAGIC         ELSE 4
# MAGIC     END;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     is_suspected_fraud,
# MAGIC     COUNT(*) AS transactions,
# MAGIC     ROUND(SUM(amount), 2) AS transaction_amount
# MAGIC FROM fraud_dev.gold.fraud_transaction_decisions
# MAGIC GROUP BY is_suspected_fraud;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM fraud_dev.gold.customer_fraud_summary
# MAGIC ORDER BY
# MAGIC     suspected_fraud_amount DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM fraud_dev.gold.merchant_fraud_summary
# MAGIC ORDER BY
# MAGIC     suspected_fraud_percentage DESC,
# MAGIC     suspected_fraud_transactions DESC
# MAGIC LIMIT 20;

# COMMAND ----------

for table_name in [
    "fraud_dev.gold.fraud_transaction_decisions",
    "fraud_dev.gold.customer_fraud_summary",
    "fraud_dev.gold.merchant_fraud_summary"
]:

    print("\nTABLE:", table_name)

    print(
        spark.table(
            table_name
        ).columns
    )