# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

SOURCE_TABLE = "fraud_dev.bronze.transactions"

TARGET_TABLE = "fraud_dev.silver.transactions"

QUARANTINE_TABLE = (
    "fraud_dev.quarantine.transaction_rejects"
)

CHECKPOINT_PATH = (
    "/Volumes/fraud_dev/staging/source_files/"
    "checkpoints/silver_transactions_v2/"
)

print("Source     :", SOURCE_TABLE)
print("Target     :", TARGET_TABLE)
print("Quarantine :", QUARANTINE_TABLE)
print("Checkpoint :", CHECKPOINT_PATH)

# COMMAND ----------

# DBTITLE 1,Define the Silver micro-batch function
def process_silver_batch(batch_df, batch_id):

    print()
    print("====================================")
    print(f"Processing Silver batch: {batch_id}")
    print("====================================")


    # ==================================================
    # 1. TYPE / NORMALIZE SOURCE DATA
    # ==================================================

    typed_batch = (
        batch_df

        .withColumn(
            "transaction_timestamp",
            F.to_timestamp(
                "transaction_timestamp"
            )
        )

        .withColumn(
            "amount",
            F.col(
                "amount"
            ).cast("double")
        )

        .withColumn(
            "latitude",
            F.col(
                "latitude"
            ).cast("double")
        )

        .withColumn(
            "longitude",
            F.col(
                "longitude"
            ).cast("double")
        )

        .withColumn(
            "currency_code",
            F.upper(
                F.trim(
                    "currency_code"
                )
            )
        )

        .withColumn(
            "transaction_status",
            F.upper(
                F.trim(
                    "transaction_status"
                )
            )
        )

        .withColumn(
            "payment_method",
            F.upper(
                F.trim(
                    "payment_method"
                )
            )
        )
    )


    # ==================================================
    # 2. DATA QUALITY RULES
    # ==================================================

    validated_batch = (
        typed_batch

        .withColumn(
            "_dq_error",

            F.when(
                F.col(
                    "transaction_id"
                ).isNull()
                |
                (
                    F.trim(
                        F.col(
                            "transaction_id"
                        )
                    ) == ""
                ),
                F.lit(
                    "MISSING_TRANSACTION_ID"
                )
            )

            .when(
                F.col(
                    "customer_id"
                ).isNull()
                |
                (
                    F.trim(
                        F.col(
                            "customer_id"
                        )
                    ) == ""
                ),
                F.lit(
                    "MISSING_CUSTOMER_ID"
                )
            )

            .when(
                F.col(
                    "merchant_id"
                ).isNull()
                |
                (
                    F.trim(
                        F.col(
                            "merchant_id"
                        )
                    ) == ""
                ),
                F.lit(
                    "MISSING_MERCHANT_ID"
                )
            )

            .when(
                F.col(
                    "transaction_timestamp"
                ).isNull(),
                F.lit(
                    "INVALID_TRANSACTION_TIMESTAMP"
                )
            )

            .when(
                F.col(
                    "amount"
                ).isNull(),
                F.lit(
                    "INVALID_AMOUNT"
                )
            )

            .when(
                F.col(
                    "amount"
                ) <= 0,
                F.lit(
                    "NON_POSITIVE_AMOUNT"
                )
            )

            .when(
                ~F.col(
                    "transaction_status"
                ).isin(
                    "SUCCESS",
                    "FAILED",
                    "DECLINED",
                    "PENDING"
                ),
                F.lit(
                    "INVALID_TRANSACTION_STATUS"
                )
            )

            .when(
                F.col(
                    "currency_code"
                ) != "INR",
                F.lit(
                    "UNSUPPORTED_CURRENCY"
                )
            )

            .otherwise(
                F.lit(
                    None
                ).cast("string")
            )
        )
    )


    # ==================================================
    # 3. VALID ROWS
    # ==================================================

    valid_batch = (
        validated_batch

        .filter(
            F.col(
                "_dq_error"
            ).isNull()
        )

        .drop(
            "_dq_error"
        )

        # protects duplicates occurring inside
        # the same micro-batch
        .dropDuplicates(
            ["transaction_id"]
        )

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
            "device_id",
            "ip_address",
            "latitude",
            "longitude",
            "country_code",

            "_ingestion_timestamp",
            "_source_file",
            "_pipeline_run_id",
            "_load_type"
        )
    )


    # ==================================================
    # 4. REJECTED ROWS
    # ==================================================

    reject_batch = (
        validated_batch

        .filter(
            F.col(
                "_dq_error"
            ).isNotNull()
        )

        .withColumn(
            "_quarantine_timestamp",
            F.current_timestamp()
        )
    )


    valid_count = (
        valid_batch.count()
    )

    reject_count = (
        reject_batch.count()
    )

    print(
        "Valid candidate rows:",
        valid_count
    )

    print(
        "Rejected rows:",
        reject_count
    )


    # ==================================================
    # 5. SILVER BUSINESS-KEY MERGE
    # ==================================================

    if valid_count > 0:

        if spark.catalog.tableExists(
            TARGET_TABLE
        ):

            silver_delta = (
                DeltaTable.forName(
                    spark,
                    TARGET_TABLE
                )
            )

            (
                silver_delta.alias("t")

                .merge(
                    valid_batch.alias("s"),

                    """
                    t.transaction_id
                    =
                    s.transaction_id
                    """
                )

                # IMPORTANT:
                # Existing trusted transaction stays untouched.
                # Only completely new transaction IDs are inserted.

                .whenNotMatchedInsertAll()

                .execute()
            )

            print(
                "Silver MERGE completed."
            )

        else:

            (
                valid_batch.write

                .format("delta")

                .mode("append")

                .saveAsTable(
                    TARGET_TABLE
                )
            )

            print(
                "Silver table created."
            )


    # ==================================================
    # 6. QUARANTINE
    # ==================================================

    if reject_count > 0:

        (
            reject_batch.write

            .format("delta")

            .mode("append")

            .saveAsTable(
                QUARANTINE_TABLE
            )
        )

        print(
            "Rejected rows written "
            "to quarantine."
        )


    print(
        f"Batch {batch_id} completed."
    )

# COMMAND ----------

# DBTITLE 1,Start the hardened Silver stream
silver_query = (
    spark.readStream
    .option("ignoreDeletes", "true")
    .table(SOURCE_TABLE)

    .writeStream
    .foreachBatch(
        process_silver_batch
    )

    .option(
        "checkpointLocation",
        CHECKPOINT_PATH
    )

    .trigger(
        availableNow=True
    )

    .start()
)

silver_query.awaitTermination()

print(
    "Silver transaction stream completed."
)

# COMMAND ----------

silver_count = (
    spark.table(
        TARGET_TABLE
    )
    .count()
)

print(
    "Silver row count:",
    silver_count
)

# COMMAND ----------

silver_count = (
    spark.table(
        TARGET_TABLE
    )
    .count()
)

print(
    "Silver row count:",
    silver_count
)

# COMMAND ----------

silver_distinct_count = (
    spark.table(
        TARGET_TABLE
    )
    .select(
        "transaction_id"
    )
    .distinct()
    .count()
)

print(
    "Distinct transaction IDs:",
    silver_distinct_count
)

# COMMAND ----------

duplicate_count = (
    spark.table(
        TARGET_TABLE
    )

    .groupBy(
        "transaction_id"
    )

    .count()

    .filter(
        F.col("count") > 1
    )

    .count()
)

print(
    "Duplicate Silver transaction IDs:",
    duplicate_count
)

# COMMAND ----------

if (
    silver_count == 250000
    and
    silver_distinct_count == 250000
    and
    duplicate_count == 0
):

    print(
        "PASS: Hardened Silver MERGE is idempotent."
    )

else:

    print(
        "FAIL: Review Silver counts before proceeding."
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS silver_rows
# MAGIC FROM fraud_dev.silver.transactions;