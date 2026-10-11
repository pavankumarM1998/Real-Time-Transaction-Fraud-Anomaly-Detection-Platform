# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

labels_df = (
    spark.table("fraud_dev.bronze.fraud_labels")
    .select(
        "transaction_id",
        "customer_id",
        "merchant_id",
        F.to_timestamp("transaction_timestamp").alias("transaction_timestamp"),
        F.col("amount").cast("double").alias("amount"),
        F.col("amount_vs_customer_avg").cast("double").alias(
            "amount_vs_customer_avg"
        ),
        F.col("is_fraud").cast("int").alias("is_fraud")
    )
    .dropna(
        subset=[
            "transaction_id",
            "customer_id",
            "merchant_id",
            "transaction_timestamp",
            "amount",
            "is_fraud"
        ]
    )
)

# COMMAND ----------

labels_df = labels_df.withColumn(
    "_event_seconds",
    F.col("transaction_timestamp").cast("long")
)

# COMMAND ----------

window_5m = (
    Window
    .partitionBy("customer_id")
    .orderBy("_event_seconds")
    .rangeBetween(-300, 0)
)

window_10m = (
    Window
    .partitionBy("customer_id")
    .orderBy("_event_seconds")
    .rangeBetween(-600, 0)
)

# COMMAND ----------

labels_features_df = (
    labels_df

    .withColumn(
        "velocity_5m",
        F.count("*").over(window_5m)
    )

    .withColumn(
        "velocity_10m",
        F.count("*").over(window_10m)
    )
)

# COMMAND ----------

merchant_features_df = (
    spark.table("fraud_dev.silver.merchants")
    .select(
        "merchant_id",
        "fraud_rate",
        "chargeback_rate"
    )
)

# COMMAND ----------

training_df = (
    labels_features_df.alias("l")

    .join(
        merchant_features_df.alias("m"),
        on="merchant_id",
        how="left"
    )

    .select(
        "transaction_timestamp",
        "amount",
        F.col("velocity_5m").cast("double").alias("velocity_5m"),
        F.col("velocity_10m").cast("double").alias("velocity_10m"),
        "amount_vs_customer_avg",
        F.col("fraud_rate").cast("double").alias("fraud_rate"),
        F.col("chargeback_rate").cast("double").alias("chargeback_rate"),
        "is_fraud"
    )

    .dropna()
)

# COMMAND ----------

training_df.printSchema()

training_df.groupBy("is_fraud").count().show()

# COMMAND ----------

training_df.select(
    "velocity_5m",
    "velocity_10m",
    "amount_vs_customer_avg",
    "fraud_rate",
    "chargeback_rate"
).summary().show()

# COMMAND ----------

pdf = (
    training_df
    .orderBy("transaction_timestamp")
    .toPandas()
)

# COMMAND ----------

feature_columns = [
    "amount",
    "velocity_5m",
    "velocity_10m",
    "amount_vs_customer_avg",
    "fraud_rate",
    "chargeback_rate"
]

# COMMAND ----------

split_index = int(len(pdf) * 0.80)

train_pdf = pdf.iloc[:split_index]
test_pdf = pdf.iloc[split_index:]

X_train = train_pdf[feature_columns]
y_train = train_pdf["is_fraud"]

X_test = test_pdf[feature_columns]
y_test = test_pdf["is_fraud"]

# COMMAND ----------

# DBTITLE 1,Train
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression

corrected_model = Pipeline([
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

corrected_model.fit(
    X_train,
    y_train
)

# COMMAND ----------

# DBTITLE 1,Evaluate
from sklearn.metrics import (
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    average_precision_score,
    confusion_matrix
)

y_pred = corrected_model.predict(X_test)
y_prob = corrected_model.predict_proba(X_test)[:, 1]

precision_v2 = precision_score(y_test, y_pred, zero_division=0)
recall_v2 = recall_score(y_test, y_pred, zero_division=0)
f1_v2 = f1_score(y_test, y_pred, zero_division=0)
roc_auc_v2 = roc_auc_score(y_test, y_prob)
pr_auc_v2 = average_precision_score(y_test, y_prob)

print("Precision :", precision_v2)
print("Recall    :", recall_v2)
print("F1        :", f1_v2)
print("ROC-AUC   :", roc_auc_v2)
print("PR-AUC    :", pr_auc_v2)

print(confusion_matrix(y_test, y_pred))

# COMMAND ----------

# DBTITLE 1,Log as a new MLflow run
import mlflow
import mlflow.sklearn

mlflow.set_experiment(
    "/Shared/fraud_detection_training"
)

with mlflow.start_run(
    run_name="logistic_regression_online_feature_contract"
) as run:

    mlflow.log_param(
        "feature_contract",
        ",".join(feature_columns)
    )

    mlflow.log_param(
        "validation_type",
        "chronological"
    )

    mlflow.log_metric("precision", precision_v2)
    mlflow.log_metric("recall", recall_v2)
    mlflow.log_metric("f1", f1_v2)
    mlflow.log_metric("roc_auc", roc_auc_v2)
    mlflow.log_metric("pr_auc", pr_auc_v2)

    mlflow.sklearn.log_model(
        sk_model=corrected_model,
        artifact_path="fraud_model"
    )

    CORRECTED_RUN_ID = run.info.run_id

print("Corrected MLflow Run ID:", CORRECTED_RUN_ID)

# COMMAND ----------

from sklearn.metrics import precision_recall_curve
import numpy as np

precision_values, recall_values, thresholds = precision_recall_curve(
    y_test,
    y_prob
)

threshold_df = []

for i, threshold in enumerate(thresholds):

    threshold_df.append(
        (
            float(threshold),
            float(precision_values[i]),
            float(recall_values[i])
        )
    )

# COMMAND ----------

candidates = [
    row
    for row in threshold_df
    if row[2] >= 0.95
]

best_threshold_row = max(
    candidates,
    key=lambda x: x[1]
)

BEST_THRESHOLD = best_threshold_row[0]

print(
    "Selected threshold:",
    BEST_THRESHOLD
)

print(
    "Precision:",
    best_threshold_row[1]
)

print(
    "Recall:",
    best_threshold_row[2]
)

# COMMAND ----------

y_pred_tuned = (
    y_prob >= BEST_THRESHOLD
).astype(int)

# COMMAND ----------

precision_tuned = precision_score(
    y_test,
    y_pred_tuned,
    zero_division=0
)

recall_tuned = recall_score(
    y_test,
    y_pred_tuned,
    zero_division=0
)

f1_tuned = f1_score(
    y_test,
    y_pred_tuned,
    zero_division=0
)

print("Threshold :", BEST_THRESHOLD)
print("Precision :", precision_tuned)
print("Recall    :", recall_tuned)
print("F1        :", f1_tuned)

print(
    confusion_matrix(
        y_test,
        y_pred_tuned
    )
)

# COMMAND ----------

for threshold in [
    0.50,
    0.60,
    0.70,
    0.80,
    0.90
]:

    pred = (
        y_prob >= threshold
    ).astype(int)

    print(
        "\nThreshold:",
        threshold
    )

    print(
        "Precision:",
        precision_score(
            y_test,
            pred,
            zero_division=0
        )
    )

    print(
        "Recall:",
        recall_score(
            y_test,
            pred,
            zero_division=0
        )
    )

    print(
        "F1:",
        f1_score(
            y_test,
            pred,
            zero_division=0
        )
    )

# COMMAND ----------

# DBTITLE 1,Register the corrected run
import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient
from mlflow.models import infer_signature
import time

mlflow.set_registry_uri("databricks-uc")

MODEL_NAME = "fraud_dev.ml.fraud_detection_model"

signature = infer_signature(X_train, corrected_model.predict(X_train))

with mlflow.start_run(run_name="logistic_regression_online_feature_contract_signed") as run:
    mlflow.sklearn.log_model(
        sk_model=corrected_model,
        artifact_path="fraud_model",
        signature=signature
    )
    SIGNED_RUN_ID = run.info.run_id

model_uri = f"runs:/{SIGNED_RUN_ID}/fraud_model"

registered_model = mlflow.register_model(
    model_uri=model_uri,
    name=MODEL_NAME
)

NEW_MODEL_VERSION = str(registered_model.version)

print("New model version:", NEW_MODEL_VERSION)

# COMMAND ----------

client = MlflowClient()

for _ in range(30):

    mv = client.get_model_version(
        name=MODEL_NAME,
        version=NEW_MODEL_VERSION
    )

    print("Status:", mv.status)

    if str(mv.status).upper() == "READY":
        break

    time.sleep(2)

# COMMAND ----------

# DBTITLE 1,Store the model contract and threshold in MLflow
client.set_model_version_tag(
    name=MODEL_NAME,
    version=NEW_MODEL_VERSION,
    key="decision_threshold",
    value=str(BEST_THRESHOLD)
)

client.set_model_version_tag(
    name=MODEL_NAME,
    version=NEW_MODEL_VERSION,
    key="feature_contract",
    value=(
        "amount,velocity_5m,velocity_10m,"
        "amount_vs_customer_avg,fraud_rate,chargeback_rate"
    )
)

client.set_model_version_tag(
    name=MODEL_NAME,
    version=NEW_MODEL_VERSION,
    key="validation_type",
    value="chronological"
)

client.set_model_version_tag(
    name=MODEL_NAME,
    version=NEW_MODEL_VERSION,
    key="status",
    value="APPROVED"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE fraud_dev.control.model_status
# MAGIC ADD COLUMNS (
# MAGIC     decision_threshold DOUBLE
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Insert the new active model first
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, DoubleType, TimestampType
)

new_model_status_df = spark.createDataFrame(
    [(
        MODEL_NAME,
        NEW_MODEL_VERSION,
        "APPROVED",
        True,
        "PR_AUC",
        float(pr_auc_v2),
        datetime.now(),
        datetime.now(),
        None,
        float(BEST_THRESHOLD)
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
        StructField("failure_reason", StringType(), True),
        StructField("decision_threshold", DoubleType(), True)
    ])
)

(
    new_model_status_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(
        "fraud_dev.control.model_status"
    )
)

# COMMAND ----------

# DBTITLE 1,Deactivate only the older versions
from delta.tables import DeltaTable
from pyspark.sql import functions as F

model_status_delta = DeltaTable.forName(
    spark,
    "fraud_dev.control.model_status"
)

model_status_delta.update(
    condition=(
        (F.col("model_name") == MODEL_NAME)
        &
        (F.col("model_version") != NEW_MODEL_VERSION)
        &
        (F.col("is_active") == True)
    ),
    set={
        "is_active": F.lit(False),
        "model_status": F.lit("INACTIVE")
    }
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     model_name,
# MAGIC     model_version,
# MAGIC     model_status,
# MAGIC     is_active,
# MAGIC     validation_metric_name,
# MAGIC     validation_metric_value,
# MAGIC     decision_threshold,
# MAGIC     activated_at
# MAGIC FROM fraud_dev.control.model_status
# MAGIC ORDER BY activated_at DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS active_model_count
# MAGIC FROM fraud_dev.control.model_status
# MAGIC WHERE model_name = 'fraud_dev.ml.fraud_detection_model'
# MAGIC   AND is_active = true;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     model_version,
# MAGIC     decision_threshold
# MAGIC FROM fraud_dev.control.model_status
# MAGIC WHERE model_name = 'fraud_dev.ml.fraud_detection_model'
# MAGIC   AND is_active = true;