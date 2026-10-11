# Databricks notebook source
# DBTITLE 1,Load the complete fraud feature table
from pyspark.sql import functions as F
import mlflow
import pandas as pd

features_df = spark.table(
    "fraud_dev.silver.transaction_complete_fraud_features"
)

# COMMAND ----------

SIMULATE_MODEL_FAILURE = False

# COMMAND ----------

# DBTITLE 1,resolve the active model and threshold
active_model_row = (
    spark.table("fraud_dev.control.model_status")
    .filter(
        (F.col("model_name") == "fraud_dev.ml.fraud_detection_model")
        &
        (F.col("is_active") == True)
        &
        (F.col("model_status") == "APPROVED")
    )
    .select(
        "model_name",
        "model_version",
        "decision_threshold"
    )
    .orderBy(F.col("activated_at").desc())
    .limit(1)
    .collect()
)

# COMMAND ----------

# DBTITLE 1,Validate that an active model exists
if not active_model_row:
    ACTIVE_MODEL_NAME = None
    ACTIVE_MODEL_VERSION = None
    DECISION_THRESHOLD = None
else:
    ACTIVE_MODEL_NAME = active_model_row[0]["model_name"]
    ACTIVE_MODEL_VERSION = active_model_row[0]["model_version"]
    DECISION_THRESHOLD = float(
        active_model_row[0]["decision_threshold"]
    )

print("Active model:", ACTIVE_MODEL_NAME)
print("Version:", ACTIVE_MODEL_VERSION)
print("Threshold:", DECISION_THRESHOLD)

# COMMAND ----------

MODEL_FEATURES = [
    "amount",
    "txn_count_5m",
    "txn_count_10m",
    "amount_vs_customer_avg",
    "fraud_rate",
    "chargeback_rate"
]

# COMMAND ----------

# DBTITLE 1,Rename the streaming/live columns to match the model contract
scoring_df = (
    features_df

    .withColumnRenamed(
        "txn_count_5m",
        "velocity_5m"
    )

    .withColumnRenamed(
        "txn_count_10m",
        "velocity_10m"
    )
)

# COMMAND ----------

# DBTITLE 1,Now the actual model contract becomes
MODEL_FEATURES = [
    "amount",
    "velocity_5m",
    "velocity_10m",
    "amount_vs_customer_avg",
    "fraud_rate",
    "chargeback_rate"
]

# COMMAND ----------

# DBTITLE 1,Before ML scoring, protect against missing feature values
scoring_df = scoring_df.fillna({
    "velocity_5m": 0.0,
    "velocity_10m": 0.0,
    "amount_vs_customer_avg": 0.0,
    "fraud_rate": 0.0,
    "chargeback_rate": 0.0
})

# COMMAND ----------

# DBTITLE 1,Now load the ML model
ML_AVAILABLE = False
loaded_model = None
ml_failure_reason = None

try:

    # --------------------------------------------------
    # FAILURE TEST SWITCH
    # --------------------------------------------------

    if SIMULATE_MODEL_FAILURE:

        raise Exception(
            "SIMULATED_MODEL_UNAVAILABLE"
        )


    # --------------------------------------------------
    # NORMAL ACTIVE MODEL VALIDATION
    # --------------------------------------------------

    if ACTIVE_MODEL_NAME is None:

        raise Exception(
            "NO_ACTIVE_APPROVED_MODEL"
        )


    # --------------------------------------------------
    # LOAD MODEL FROM UNITY CATALOG
    # --------------------------------------------------

    mlflow.set_registry_uri(
        "databricks-uc"
    )

    MODEL_URI = (
        f"models:/{ACTIVE_MODEL_NAME}/"
        f"{ACTIVE_MODEL_VERSION}"
    )

    loaded_model = (
        mlflow.sklearn.load_model(
            MODEL_URI
        )
    )

    ML_AVAILABLE = True

    print(
        "ML model successfully loaded:",
        MODEL_URI
    )


except Exception as e:

    ML_AVAILABLE = False

    ml_failure_reason = str(e)

    print(
        "ML unavailable."
    )

    print(
        "Failure reason:",
        ml_failure_reason
    )

    print(
        "Rule-engine fallback will be used."
    )

# COMMAND ----------

# DBTITLE 1,If ML is available
if ML_AVAILABLE:

    scoring_pdf = (
        scoring_df
        .select(
            "transaction_id",
            *MODEL_FEATURES
        )
        .toPandas()
    )

    probability = (
        loaded_model
        .predict_proba(
            scoring_pdf[MODEL_FEATURES]
        )[:, 1]
    )

    scoring_pdf["ml_fraud_probability"] = (
        probability
    )

    scoring_pdf["ml_fraud_prediction"] = (
        scoring_pdf[
            "ml_fraud_probability"
        ]
        >= DECISION_THRESHOLD
    ).astype(int)

    ml_results_df = spark.createDataFrame(
        scoring_pdf[
            [
                "transaction_id",
                "ml_fraud_probability",
                "ml_fraud_prediction"
            ]
        ]
    )

# COMMAND ----------

if ML_AVAILABLE:

    scored_df = (
        scoring_df

        .join(
            ml_results_df,
            on="transaction_id",
            how="left"
        )

        .withColumn(
            "model_fallback_used",
            F.lit(False)
        )

        .withColumn(
            "model_version_used",
            F.lit(
                str(ACTIVE_MODEL_VERSION)
            )
        )

        .withColumn(
            "decision_threshold_used",
            F.lit(
                float(
                    DECISION_THRESHOLD
                )
            )
        )
    )

# COMMAND ----------

# DBTITLE 1,If ML is unavailable
if not ML_AVAILABLE:

    scored_df = (
        scoring_df

        .withColumn(
            "ml_fraud_probability",
            F.lit(None).cast("double")
        )

        .withColumn(
            "ml_fraud_prediction",
            F.lit(None).cast("int")
        )

        .withColumn(
            "model_fallback_used",
            F.lit(True)
        )

        .withColumn(
            "model_version_used",
            F.lit(None).cast("string")
        )

        .withColumn(
            "decision_threshold_used",
            F.lit(None).cast("double")
        )
    )

# COMMAND ----------

# DBTITLE 1,Cell 12
if ML_AVAILABLE:

    scored_df = (
        scored_df

        .withColumn(
            "final_fraud_score",

            (
                F.col(
                    "ml_fraud_probability"
                ) * 100 * 0.70
            )

            +

            (
                F.col(
                    "rule_score"
                ) * 0.30
            )
        )
    )

else:

    scored_df = (
        scored_df

        .withColumn(
            "final_fraud_score",
            F.col("rule_score").cast("double")
        )
    )

# COMMAND ----------

scored_df = (
    scored_df

    .withColumn(
        "final_risk_level",

        F.when(
            F.col("final_fraud_score") >= 80,
            "CRITICAL"
        )

        .when(
            F.col("final_fraud_score") >= 60,
            "HIGH"
        )

        .when(
            F.col("final_fraud_score") >= 30,
            "MEDIUM"
        )

        .otherwise("LOW")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC If ML is available, we want either a strong ML prediction or an extreme deterministic rule score to flag the transaction:

# COMMAND ----------

# DBTITLE 1,Cell 15
if ML_AVAILABLE:

    scored_df = (
        scored_df

        .withColumn(
            "final_fraud_prediction",

            F.when(
                (
                    F.col(
                        "ml_fraud_prediction"
                    ) == 1
                )
                |
                (
                    F.col(
                        "rule_score"
                    ) >= 80
                ),
                1
            )

            .otherwise(0)
        )
    )

else:

    scored_df = (
        scored_df

        .withColumn(
            "final_fraud_prediction",

            F.when(
                F.col("rule_score") >= 80,
                1
            )

            .otherwise(0)
        )
    )

# COMMAND ----------

scored_df = scored_df.withColumn(
    "_scored_at",
    F.current_timestamp()
)

# COMMAND ----------

TARGET_TABLE = (
    "fraud_dev.silver.transaction_fraud_scored"
)

(
    scored_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        TARGET_TABLE
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_rows
# MAGIC FROM fraud_dev.silver.transaction_fraud_scored;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     model_fallback_used,
# MAGIC     COUNT(*) AS transactions
# MAGIC FROM fraud_dev.silver.transaction_fraud_scored
# MAGIC GROUP BY model_fallback_used;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     final_risk_level,
# MAGIC     COUNT(*) AS transactions
# MAGIC FROM fraud_dev.silver.transaction_fraud_scored
# MAGIC GROUP BY final_risk_level
# MAGIC ORDER BY transactions DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     final_fraud_prediction,
# MAGIC     COUNT(*) AS transactions
# MAGIC FROM fraud_dev.silver.transaction_fraud_scored
# MAGIC GROUP BY final_fraud_prediction;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     transaction_id,
# MAGIC     customer_id,
# MAGIC     merchant_id,
# MAGIC     amount,
# MAGIC     ml_fraud_probability,
# MAGIC     rule_score,
# MAGIC     final_fraud_score,
# MAGIC     final_fraud_prediction,
# MAGIC     final_risk_level,
# MAGIC     model_version_used,
# MAGIC     model_fallback_used
# MAGIC FROM fraud_dev.silver.transaction_fraud_scored
# MAGIC ORDER BY final_fraud_score DESC
# MAGIC LIMIT 50;