# Databricks notebook source
from pyspark.sql import functions as F
from datetime import datetime
import re
import uuid

# COMMAND ----------

# DBTITLE 1,Define supported entities
BASE_PATH = "/Volumes/fraud_dev/staging/source_files"

ENTITY_CONFIG = {

    "customers": {
        "expected_columns": [
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
            "updated_at"
        ],

        "mandatory_columns": [
            "customer_id",
            "customer_name",
            "customer_status"
        ],

        "filename_pattern": r"^customers_\d{8}\.csv$"
    },

    "merchants": {
        "expected_columns": [
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
            "updated_at"
        ],

        "mandatory_columns": [
            "merchant_id",
            "merchant_name",
            "merchant_category"
        ],

        "filename_pattern": r"^merchants_\d{8}\.csv$"
    },

    "fraud_labels": {
        "expected_columns": [
            "transaction_id",
            "customer_id",
            "merchant_id",
            "transaction_timestamp",
            "amount",
            "currency_code",
            "payment_method",
            "merchant_category",
            "device_id",
            "country_code",
            "velocity_5m",
            "velocity_30m",
            "amount_vs_customer_avg",
            "merchant_risk_score",
            "is_fraud"
        ],

        "mandatory_columns": [
            "transaction_id",
            "customer_id",
            "merchant_id",
            "transaction_timestamp",
            "amount",
            "is_fraud"
        ],

        "filename_pattern": r"^fraud_labels_\d{8}\.csv$"
    },

    "transactions": {
        "expected_columns": [
            "transaction_id",
            "customer_id",
            "merchant_id",
            "transaction_timestamp",
            "amount",
            "currency_code",
            "payment_method",
            "merchant_category",
            "transaction_status",
            "device_id",
            "ip_address",
            "latitude",
            "longitude",
            "country_code"
        ],

        "mandatory_columns": [
            "transaction_id",
            "customer_id",
            "merchant_id",
            "transaction_timestamp",
            "amount",
            "currency_code",
            "transaction_status"
        ],

        "filename_pattern": (
            r"^transactions_"
            r"(initial_\d{8}_\d{3}"
            r"|\d{8}_\d{6}_\d{3})"
            r"\.csv$"
        )
    }
}

# COMMAND ----------

# DBTITLE 1,Add notebook parameter
dbutils.widgets.text("ENTITY", "transactions")

ENTITY = dbutils.widgets.get("ENTITY").strip().lower()

if ENTITY not in ENTITY_CONFIG:
    raise ValueError(
        f"Unsupported ENTITY: {ENTITY}. "
        f"Valid entities: {list(ENTITY_CONFIG.keys())}"
    )

print("Validating entity:", ENTITY)

# COMMAND ----------

# DBTITLE 1,Paths
incoming_path = f"{BASE_PATH}/{ENTITY}/incoming"
rejected_path = f"{BASE_PATH}/{ENTITY}/rejected"

print("Incoming:", incoming_path)
print("Rejected:", rejected_path)

# COMMAND ----------

# DBTITLE 1,Generate pipeline run ID
PIPELINE_RUN_ID = str(uuid.uuid4())

print("Pipeline Run ID:", PIPELINE_RUN_ID)

# COMMAND ----------

# DBTITLE 1,Discover incoming files
all_files = [
    f
    for f in dbutils.fs.ls(incoming_path)
    if not f.isDir()
]

registered_paths = {
    r["file_path"]
    for r in (
        spark.table(
            "fraud_dev.control.file_processing_status"
        )
        .filter(
            (F.col("entity_name") == ENTITY) &
            (F.col("validation_status") == "PASS") &
            (
                F.col("processing_status").isin(
                    "READY",
                    "BRONZE_LOADED",
                    "SILVER_LOADED",
                    "PROCESSED"
                )
            )
        )
        .select("file_path")
        .distinct()
        .collect()
    )
}

files = [
    f for f in all_files
    if f.path not in registered_paths
]

print("Files physically present :", len(all_files))
print("Already registered       :", len(all_files) - len(files))
print("New files to validate    :", len(files))

for f in files:
    print(
        f.name,
        "| size:",
        f.size,
        "| modified:",
        f.modificationTime
    )

# COMMAND ----------

if len(files) == 0:
    print(f"No incoming files for {ENTITY}. Valid no-data run.")

# COMMAND ----------

# DBTITLE 1,Helper: previously processed file check
def already_processed(file_path):

    result = (
        spark.table("fraud_dev.control.file_processing_status")
        .filter(
            (F.col("file_path") == file_path) &
            (F.col("processing_status") == "PROCESSED")
        )
        .limit(1)
        .count()
    )

    return result > 0

# COMMAND ----------

# DBTITLE 1,Helper: read header only
def get_csv_columns(file_path):

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(file_path)
    )

    return df.columns

# COMMAND ----------

# DBTITLE 1,Additional entity-specific structural checks
def validate_basic_formats(entity, file_path):

    errors = []

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(file_path)
    )

    # ----------------------------------
    # Transactions / fraud labels
    # ----------------------------------

    if entity in ["transactions", "fraud_labels"]:

        if "amount" in df.columns:

            invalid_amount = (
                df
                .filter(
                    F.col("amount").isNotNull() &
                    F.col("amount").cast("double").isNull()
                )
                .limit(1)
                .count()
            )

            if invalid_amount:
                errors.append("AMOUNT_NOT_NUMERIC")

        if "transaction_timestamp" in df.columns:

            invalid_timestamp = (
                df
                .filter(
                    F.col("transaction_timestamp").isNotNull() &
                    F.to_timestamp(
                        "transaction_timestamp"
                    ).isNull()
                )
                .limit(1)
                .count()
            )

            if invalid_timestamp:
                errors.append(
                    "INVALID_TRANSACTION_TIMESTAMP_FORMAT"
                )

    # ----------------------------------
    # Customers
    # ----------------------------------

    if entity == "customers":

        for c in [
            "avg_monthly_spend",
            "avg_transaction_amount"
        ]:

            if c in df.columns:

                invalid_numeric = (
                    df
                    .filter(
                        F.col(c).isNotNull() &
                        F.col(c).cast("double").isNull()
                    )
                    .limit(1)
                    .count()
                )

                if invalid_numeric:
                    errors.append(
                        f"{c.upper()}_NOT_NUMERIC"
                    )

    # ----------------------------------
    # Merchants
    # ----------------------------------

    if entity == "merchants":

        for c in [
            "chargeback_rate",
            "fraud_rate"
        ]:

            if c in df.columns:

                invalid_numeric = (
                    df
                    .filter(
                        F.col(c).isNotNull() &
                        F.col(c).cast("double").isNull()
                    )
                    .limit(1)
                    .count()
                )

                if invalid_numeric:
                    errors.append(
                        f"{c.upper()}_NOT_NUMERIC"
                    )

    return errors

# COMMAND ----------

# DBTITLE 1,Execute validation
def validate_file(entity, file_info):

    errors = []

    file_path = file_info.path
    file_name = file_info.name
    file_size = file_info.size

    config = ENTITY_CONFIG[entity]

    if not re.match(config["filename_pattern"], file_name):
        errors.append("FILENAME_PATTERN_MISMATCH")

    try:
        columns = get_csv_columns(file_path)
    except Exception:
        errors.append("FILE_NOT_READABLE")
        return {
            "file_path": file_path,
            "file_name": file_name,
            "entity_name": entity,
            "file_size": file_size,
            "validation_status": "FAIL",
            "row_count": 0,
            "errors": errors
        }

    missing_columns = [
        c for c in config["expected_columns"]
        if c not in columns
    ]

    if missing_columns:
        errors.append(
            "MISSING_COLUMNS: " + ", ".join(missing_columns)
        )

    missing_mandatory = [
        c for c in config["mandatory_columns"]
        if c not in columns
    ]

    if missing_mandatory:
        errors.append(
            "MISSING_MANDATORY_COLUMNS: " + ", ".join(missing_mandatory)
        )

    row_count = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(file_path)
        .count()
    )

    validation_status = "PASS" if not errors else "FAIL"

    return {
        "file_path": file_path,
        "file_name": file_name,
        "entity_name": entity,
        "file_size": file_size,
        "validation_status": validation_status,
        "row_count": row_count,
        "errors": errors
    }


validation_results = []

for file_info in files:

    result = validate_file(
        ENTITY,
        file_info
    )

    if result["validation_status"] == "PASS":

        format_errors = validate_basic_formats(
            ENTITY,
            result["file_path"]
        )

        if format_errors:
            result["validation_status"] = "FAIL"
            result["errors"].extend(format_errors)

    validation_results.append(result)

# COMMAND ----------

for result in validation_results:

    print("=" * 80)

    print("File:", result["file_name"])
    print("Rows:", result["row_count"])
    print("Status:", result["validation_status"])

    if result["errors"]:
        print("Errors:")
        for error in result["errors"]:
            print(" -", error)

# COMMAND ----------

# DBTITLE 1,Write validation audit
audit_rows = []

for result in validation_results:

    failure_reason = (
        " | ".join(result["errors"])
        if result["errors"]
        else None
    )

    audit_rows.append((
        result["file_path"],
        result["file_name"],
        result["entity_name"],
        int(result["file_size"]),
        None,
        result["validation_status"],
        (
            "READY"
            if result["validation_status"] == "PASS"
            else "REJECTED"
        ),
        int(result["row_count"]),
        PIPELINE_RUN_ID,
        failure_reason,
        datetime.now(),
        datetime.now(),
        None,
        None
    ))

# COMMAND ----------

from pyspark.sql.types import *

audit_schema = StructType([
    StructField("file_path", StringType()),
    StructField("file_name", StringType()),
    StructField("entity_name", StringType()),
    StructField("file_size", LongType()),
    StructField("file_modification_time", TimestampType()),

    StructField("validation_status", StringType()),
    StructField("processing_status", StringType()),

    StructField("row_count", LongType()),

    StructField("pipeline_run_id", StringType()),

    StructField("failure_reason", StringType()),

    StructField("discovered_at", TimestampType()),
    StructField("validated_at", TimestampType()),
    StructField("processed_at", TimestampType()),
    StructField("archived_at", TimestampType())
])

# COMMAND ----------

if audit_rows:

    audit_df = spark.createDataFrame(
        audit_rows,
        audit_schema
    )

    (
        audit_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(
            "fraud_dev.control.file_processing_status"
        )
    )

# COMMAND ----------

# DBTITLE 1,Move failed files to rejected
for result in validation_results:

    if result["validation_status"] == "FAIL":

        source = result["file_path"]

        target = (
            f"{rejected_path}/"
            f"{PIPELINE_RUN_ID}_"
            f"{result['file_name']}"
        )

        print(
            f"Rejecting:\n"
            f"{source}\n"
            f"→ {target}"
        )

        dbutils.fs.mv(
            source,
            target
        )

# COMMAND ----------

# DBTITLE 1,Final validation gate
passed = [
    r for r in validation_results
    if r["validation_status"] == "PASS"
]

failed = [
    r for r in validation_results
    if r["validation_status"] == "FAIL"
]

print("=" * 80)
print("STAGING VALIDATION SUMMARY")
print("=" * 80)

print("Entity:", ENTITY)
print("Discovered:", len(validation_results))
print("Passed:", len(passed))
print("Rejected:", len(failed))
print("Pipeline Run ID:", PIPELINE_RUN_ID)

# COMMAND ----------

if failed:
    print(
        "Some files were rejected. "
        "Valid files remain eligible for processing."
    )

if passed:
    print(
        f"{len(passed)} valid file(s) ready for ingestion."
    )

# COMMAND ----------

# DBTITLE 1,Verify control table
# MAGIC %sql
# MAGIC SELECT
# MAGIC     entity_name,
# MAGIC     file_name,
# MAGIC     validation_status,
# MAGIC     processing_status,
# MAGIC     row_count,
# MAGIC     pipeline_run_id,
# MAGIC     failure_reason,
# MAGIC     validated_at
# MAGIC FROM fraud_dev.control.file_processing_status
# MAGIC ORDER BY validated_at DESC;