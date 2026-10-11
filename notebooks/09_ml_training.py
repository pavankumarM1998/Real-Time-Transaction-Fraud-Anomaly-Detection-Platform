# Databricks notebook source
from pyspark.sql import functions as F

training_df = (
    spark.table("fraud_dev.bronze.fraud_labels")

    .select(
        F.col("amount").cast("double").alias("amount"),
        F.col("velocity_5m").cast("double").alias("velocity_5m"),
        F.col("velocity_30m").cast("double").alias("velocity_30m"),
        F.col("amount_vs_customer_avg").cast("double").alias(
            "amount_vs_customer_avg"
        ),
        F.col("merchant_risk_score").cast("double").alias(
            "merchant_risk_score"
        ),
        F.col("is_fraud").cast("int").alias("is_fraud")
    )

    .dropna()
)

# COMMAND ----------

training_df.groupBy("is_fraud").count().show()

# COMMAND ----------

pdf = training_df.toPandas()

# COMMAND ----------

feature_columns = [
    "amount",
    "velocity_5m",
    "velocity_30m",
    "amount_vs_customer_avg",
    "merchant_risk_score"
]

X = pdf[feature_columns]
y = pdf["is_fraud"]

# COMMAND ----------

from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.20,
    random_state=42,
    stratify=y
)

# COMMAND ----------

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression

model = Pipeline([
    (
        "scaler",
        StandardScaler()
    ),
    (
        "classifier",
        LogisticRegression(
            class_weight="balanced",
            max_iter=1000,
            random_state=42
        )
    )
])

# COMMAND ----------

model.fit(
    X_train,
    y_train
)

# COMMAND ----------

y_pred = model.predict(X_test)

y_prob = model.predict_proba(X_test)[:, 1]

# COMMAND ----------

from sklearn.metrics import (
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    average_precision_score,
    confusion_matrix,
    classification_report
)

precision = precision_score(
    y_test,
    y_pred,
    zero_division=0
)

recall = recall_score(
    y_test,
    y_pred,
    zero_division=0
)

f1 = f1_score(
    y_test,
    y_pred,
    zero_division=0
)

roc_auc = roc_auc_score(
    y_test,
    y_prob
)

pr_auc = average_precision_score(
    y_test,
    y_prob
)

# COMMAND ----------

print("Precision :", precision)
print("Recall    :", recall)
print("F1        :", f1)
print("ROC-AUC   :", roc_auc)
print("PR-AUC    :", pr_auc)

print(
    classification_report(
        y_test,
        y_pred
    )
)

print(
    confusion_matrix(
        y_test,
        y_pred
    )
)

# COMMAND ----------

import mlflow
import mlflow.sklearn

mlflow.set_experiment(
    "/Shared/fraud_detection_training"
)

# COMMAND ----------

with mlflow.start_run(
    run_name="logistic_regression_baseline"
) as run:

    mlflow.log_param(
        "model_type",
        "LogisticRegression"
    )

    mlflow.log_param(
        "class_weight",
        "balanced"
    )

    mlflow.log_param(
        "random_state",
        42
    )

    mlflow.log_metric(
        "precision",
        precision
    )

    mlflow.log_metric(
        "recall",
        recall
    )

    mlflow.log_metric(
        "f1",
        f1
    )

    mlflow.log_metric(
        "roc_auc",
        roc_auc
    )

    mlflow.log_metric(
        "pr_auc",
        pr_auc
    )

    mlflow.sklearn.log_model(
        sk_model=model,
        artifact_path="fraud_model"
    )

    RUN_ID = run.info.run_id

print("MLflow Run ID:", RUN_ID)

# COMMAND ----------

# DBTITLE 1,Also create a small result table for comparison
metrics_df = spark.createDataFrame(
    [
        (
            RUN_ID,
            "LogisticRegression",
            float(precision),
            float(recall),
            float(f1),
            float(roc_auc),
            float(pr_auc)
        )
    ],
    [
        "mlflow_run_id",
        "model_name",
        "precision",
        "recall",
        "f1",
        "roc_auc",
        "pr_auc"
    ]
)

(
    metrics_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(
        "fraud_dev.ml.model_evaluation"
    )
)

# COMMAND ----------

# DBTITLE 1,First check feature separation
training_df.groupBy("is_fraud").agg(

    F.avg("amount").alias("avg_amount"),

    F.avg("velocity_5m").alias(
        "avg_velocity_5m"
    ),

    F.avg("velocity_30m").alias(
        "avg_velocity_30m"
    ),

    F.avg(
        "amount_vs_customer_avg"
    ).alias(
        "avg_amount_ratio"
    ),

    F.avg(
        "merchant_risk_score"
    ).alias(
        "avg_merchant_risk"
    )

).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,perform a time-based validation
time_training_df = (
    spark.table(
        "fraud_dev.bronze.fraud_labels"
    )

    .select(
        F.to_timestamp(
            "transaction_timestamp"
        ).alias("transaction_timestamp"),

        F.col("amount")
         .cast("double")
         .alias("amount"),

        F.col("velocity_5m")
         .cast("double")
         .alias("velocity_5m"),

        F.col("velocity_30m")
         .cast("double")
         .alias("velocity_30m"),

        F.col("amount_vs_customer_avg")
         .cast("double")
         .alias("amount_vs_customer_avg"),

        F.col("merchant_risk_score")
         .cast("double")
         .alias("merchant_risk_score"),

        F.col("is_fraud")
         .cast("int")
         .alias("is_fraud")
    )

    .dropna()
)

# COMMAND ----------

time_pdf = (
    time_training_df
    .orderBy("transaction_timestamp")
    .toPandas()
)

# COMMAND ----------

# DBTITLE 1,Split chronologically
split_index = int(
    len(time_pdf) * 0.80
)

train_pdf = time_pdf.iloc[:split_index]
test_pdf  = time_pdf.iloc[split_index:]

# COMMAND ----------

X_train_time = train_pdf[feature_columns]
y_train_time = train_pdf["is_fraud"]

X_test_time = test_pdf[feature_columns]
y_test_time = test_pdf["is_fraud"]

# COMMAND ----------

# DBTITLE 1,Train a fresh model
time_model = Pipeline([
    (
        "scaler",
        StandardScaler()
    ),
    (
        "classifier",
        LogisticRegression(
            class_weight="balanced",
            max_iter=1000,
            random_state=42
        )
    )
])

time_model.fit(
    X_train_time,
    y_train_time
)

# COMMAND ----------

time_pred = time_model.predict(
    X_test_time
)

time_prob = time_model.predict_proba(
    X_test_time
)[:, 1]

# COMMAND ----------

print(
    "Precision:",
    precision_score(
        y_test_time,
        time_pred
    )
)

print(
    "Recall:",
    recall_score(
        y_test_time,
        time_pred
    )
)

print(
    "F1:",
    f1_score(
        y_test_time,
        time_pred
    )
)

print(
    "ROC-AUC:",
    roc_auc_score(
        y_test_time,
        time_prob
    )
)

print(
    "PR-AUC:",
    average_precision_score(
        y_test_time,
        time_prob
    )
)

print(
    confusion_matrix(
        y_test_time,
        time_pred
    )
)

# COMMAND ----------

import mlflow

mlflow.set_registry_uri("databricks-uc")

MODEL_NAME = "fraud_dev.ml.fraud_detection_model"

print("Registry model:", MODEL_NAME)

# COMMAND ----------

# DBTITLE 1,Now register the already logged model from your earlier run
from mlflow.models import infer_signature

signature = infer_signature(X_train, model.predict(X_train))

with mlflow.start_run(run_name="logistic_regression_baseline_signed") as run:
    mlflow.sklearn.log_model(
        sk_model=model,
        artifact_path="fraud_model",
        signature=signature
    )
    SIGNED_RUN_ID = run.info.run_id

model_uri = f"runs:/{SIGNED_RUN_ID}/fraud_model"

registered_model = mlflow.register_model(
    model_uri=model_uri,
    name=MODEL_NAME
)

MODEL_VERSION = registered_model.version

print("Registered model version:", MODEL_VERSION)

# COMMAND ----------

from mlflow.tracking import MlflowClient
import time

client = MlflowClient()

for _ in range(30):
    mv = client.get_model_version(
        name=MODEL_NAME,
        version=MODEL_VERSION
    )

    print("Model status:", mv.status)

    if str(mv.status).upper() == "READY":
        break

    time.sleep(2)

# COMMAND ----------

client.update_model_version(
    name=MODEL_NAME,
    version=MODEL_VERSION,
    description=(
        "Logistic Regression fraud detection model. "
        "Features: amount, velocity_5m, velocity_30m, "
        "amount_vs_customer_avg, merchant_risk_score. "
        "Validated using random and chronological holdout datasets."
    )
)

# COMMAND ----------

client.set_model_version_tag(
    name=MODEL_NAME,
    version=MODEL_VERSION,
    key="model_type",
    value="LogisticRegression"
)

client.set_model_version_tag(
    name=MODEL_NAME,
    version=MODEL_VERSION,
    key="validation",
    value="TIME_BASED"
)

# COMMAND ----------

# DBTITLE 1,Cell 27
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, DoubleType, TimestampType
)

model_status_df = spark.createDataFrame(
    [(
        MODEL_NAME,
        str(MODEL_VERSION),
        "APPROVED",
        True,
        "PR_AUC",
        float(pr_auc),
        datetime.now(),
        datetime.now(),
        None
    )],
    StructType([
        StructField("model_name", StringType(), True),
        StructField("model_version", StringType(), True),
        StructField("model_status", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("validation_metric_name", StringType(), True),
        StructField("validation_metric_value", DoubleType(), True),
        StructField("registered_at", TimestampType(), True),
        StructField("activated_at", TimestampType(), True),
        StructField("failure_reason", StringType(), True)
    ])
)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

model_status_delta = DeltaTable.forName(
    spark,
    "fraud_dev.control.model_status"
)

(
    model_status_delta.alias("t")
    .update(
        condition=(
            (F.col("model_name") == MODEL_NAME) &
            (F.col("is_active") == True)
        ),
        set={
            "is_active": F.lit(False),
            "model_status": F.lit("INACTIVE")
        }
    )
)

# COMMAND ----------

(
    model_status_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(
        "fraud_dev.control.model_status"
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM fraud_dev.control.model_status
# MAGIC ORDER BY registered_at DESC;