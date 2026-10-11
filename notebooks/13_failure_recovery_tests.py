# Databricks notebook source
from pyspark.sql import functions as F

bronze_before = (
    spark.table("fraud_dev.bronze.transactions")
    .count()
)

silver_before = (
    spark.table("fraud_dev.silver.transactions")
    .count()
)

silver_distinct_before = (
    spark.table("fraud_dev.silver.transactions")
    .select("transaction_id")
    .distinct()
    .count()
)

quarantine_before = (
    spark.table(
        "fraud_dev.quarantine.transaction_rejects"
    )
    .count()
)

print("BEFORE REPLAY")
print("-----------------------------")
print("Bronze rows       :", bronze_before)
print("Silver rows       :", silver_before)
print("Silver distinct   :", silver_distinct_before)
print("Quarantine rows   :", quarantine_before)

# COMMAND ----------

BASE_PATH = "/Volumes/fraud_dev/staging/source_files"

INCOMING_PATH = f"{BASE_PATH}/transactions/incoming/"
ARCHIVE_PATH = f"{BASE_PATH}/transactions/archive/"
REJECTED_PATH = f"{BASE_PATH}/transactions/rejected/"

ARCHIVE_PATH = (
    f"{BASE_PATH}/transactions/archive/"
)

INCOMING_PATH = (
    f"{BASE_PATH}/transactions/incoming/"
)

REJECTED_PATH = (
    f"{BASE_PATH}/transactions/rejected/"
)

TEST_FILE_NAME = (
    "transactions_initial_20261002_001.csv"
)

ARCHIVE_FILE = (
    ARCHIVE_PATH
    + TEST_FILE_NAME
)

INCOMING_FILE = (
    INCOMING_PATH
    + TEST_FILE_NAME
)

print("Archive file :", ARCHIVE_FILE)
print("Incoming file:", INCOMING_FILE)

# COMMAND ----------

# DBTITLE 1,Cell 2
DUPLICATE_FILE = (
    INCOMING_PATH
    + "transactions_initial_20261002_001_dup.csv"
)

dbutils.fs.cp(
    INCOMING_FILE,
    DUPLICATE_FILE
)

print(
    "Duplicate file copied to incoming."
)

# COMMAND ----------

display(
    dbutils.fs.ls(
        INCOMING_PATH
    )
)

# COMMAND ----------

# DBTITLE 1,clean up
BASE_PATH = "/Volumes/fraud_dev/staging/source_files"

INCOMING_PATH = f"{BASE_PATH}/transactions/incoming/"

files = dbutils.fs.ls(INCOMING_PATH)

for f in files:

    file_name = f.name

    # remove explicit test duplicate files
    if "_dup" in file_name:

        dbutils.fs.rm(
            f.path
        )

        print(
            "Removed test duplicate:",
            file_name
        )

# COMMAND ----------

BASE_PATH = "/Volumes/fraud_dev/staging/source_files"

INCOMING_PATH = f"{BASE_PATH}/transactions/incoming/"
ARCHIVE_PATH = f"{BASE_PATH}/transactions/archive/"

incoming_files = dbutils.fs.ls(INCOMING_PATH)

archive_names = {
    f.name
    for f in dbutils.fs.ls(ARCHIVE_PATH)
}

for f in incoming_files:

    file_name = f.name

    # remove only files already safely archived
    if file_name in archive_names:

        dbutils.fs.rm(
            f.path
        )

        print(
            "Removed archived duplicate from incoming:",
            file_name
        )

    # remove explicit test files
    elif "_dup" in file_name:

        dbutils.fs.rm(
            f.path
        )

        print(
            "Removed test duplicate:",
            file_name
        )

# COMMAND ----------

display(
    dbutils.fs.ls(
        INCOMING_PATH
    )
)

# COMMAND ----------

# DBTITLE 1,Copy one archived file back with the exact same filename
BASE_PATH = "/Volumes/fraud_dev/staging/source_files"

INCOMING_PATH = f"{BASE_PATH}/transactions/incoming/"
ARCHIVE_PATH = f"{BASE_PATH}/transactions/archive/"
REJECTED_PATH = f"{BASE_PATH}/transactions/rejected/"

TEST_FILE_NAME = "transactions_initial_20261002_001.csv"

TEMP_FILE = (
    INCOMING_PATH
    + "_temp_"
    + TEST_FILE_NAME
)

dbutils.fs.cp(
    INCOMING_PATH + TEST_FILE_NAME,
    TEMP_FILE
)

dbutils.fs.cp(
    TEMP_FILE,
    INCOMING_PATH + TEST_FILE_NAME
)

dbutils.fs.rm(TEMP_FILE)

print("Duplicate test file created:", TEST_FILE_NAME)

# COMMAND ----------

display(
    dbutils.fs.ls(
        INCOMING_PATH
    )
)

# COMMAND ----------

# DBTITLE 1,Verify duplicate went to rejected
rejected_files = dbutils.fs.ls(
    REJECTED_PATH
)

display(
    rejected_files
)

# COMMAND ----------

# DBTITLE 1,Verify downstream counts did not change
bronze_count = spark.table(
    "fraud_dev.bronze.transactions"
).count()

silver_count = spark.table(
    "fraud_dev.silver.transactions"
).count()

gold_count = spark.table(
    "fraud_dev.gold.fraud_transaction_decisions"
).count()

print("Bronze :", bronze_count)
print("Silver :", silver_count)
print("Gold   :", gold_count)

# COMMAND ----------

from pyspark.sql import functions as F

bronze_duplicates = (
    spark.table("fraud_dev.bronze.transactions")
    .groupBy("transaction_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

gold_duplicates = (
    spark.table(
        "fraud_dev.gold.fraud_transaction_decisions"
    )
    .groupBy("transaction_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("Bronze duplicate IDs:", bronze_duplicates)
print("Gold duplicate IDs  :", gold_duplicates)

# COMMAND ----------

# DBTITLE 1,failure Test 2 — different valid filename, same transaction IDs
BASE_PATH = "/Volumes/fraud_dev/staging/source_files"

ARCHIVE_PATH = f"{BASE_PATH}/transactions/archive/"
INCOMING_PATH = f"{BASE_PATH}/transactions/incoming/"

SOURCE_FILE_NAME = "transactions_initial_20261002_001.csv"

TEST_FILE_NAME = "transactions_20261003_110000_999.csv"

SOURCE_FILE = ARCHIVE_PATH + SOURCE_FILE_NAME
TEST_FILE = INCOMING_PATH + TEST_FILE_NAME

print("Source :", SOURCE_FILE)
print("Test   :", TEST_FILE)

# COMMAND ----------

# DBTITLE 1,Copy the old data using a new valid filename
dbutils.fs.cp(
    INCOMING_PATH + SOURCE_FILE_NAME,
    TEST_FILE
)

print(
    "Failure Test 2 file created:",
    TEST_FILE_NAME
)

# COMMAND ----------

# DBTITLE 1,Verify incoming
display(
    dbutils.fs.ls(
        INCOMING_PATH
    )
)

# COMMAND ----------

# DBTITLE 1,Failure Test 3 — invalid transaction row → Bronze preserves it → Silver quarantines it.
# Failure Test 3 — invalid transaction row → Bronze preserves it → Silver quarantines it.

# First, create one small CSV with a new valid filename and one deliberately bad row. We can create it directly in the volume for this test.

from pyspark.sql import Row

TEST_FILE_NAME = "transactions_20261003_111500_998.csv"

TEST_FILE_PATH = (
    "/Volumes/fraud_dev/staging/source_files/"
    "transactions/incoming/"
    + TEST_FILE_NAME
)

bad_row_df = spark.createDataFrame([
    Row(
        transaction_id="TXN_BAD_0001",
        customer_id="CUST000001",
        merchant_id="MER000001",
        transaction_timestamp="2026-10-03 11:15:00",
        amount="-500.00",
        currency_code="INR",
        payment_method="UPI",
        merchant_category="GROCERY",
        transaction_status="SUCCESS",
        device_id="DEV_TEST_001",
        ip_address="10.0.0.1",
        latitude="12.9716",
        longitude="77.5946",
        country_code="IN"
    )
])

(
    bad_row_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv("/Volumes/fraud_dev/staging/source_files/transactions/_temp_bad_row_test")
)

# COMMAND ----------

# DBTITLE 1,Cell 16
# Now move the generated part file into the exact incoming filename

part_file = [
    f.path
    for f in dbutils.fs.ls("/Volumes/fraud_dev/staging/source_files/transactions/_temp_bad_row_test")
    if f.name.startswith("part-")
][0]

dbutils.fs.cp(
    part_file,
    TEST_FILE_PATH
)

dbutils.fs.rm(
    "/Volumes/fraud_dev/staging/source_files/transactions/_temp_bad_row_test",
    recurse=True
)

print("Created:", TEST_FILE_PATH)

# COMMAND ----------

display(
    dbutils.fs.ls(
        "/Volumes/fraud_dev/staging/source_files/transactions/incoming/"
    )
)

# COMMAND ----------

# DBTITLE 1,Failure Test 4 — schema failure at staging
TEST_FILE_NAME = "transactions_20261003_112500_997.csv"

TEST_FILE_PATH = (
    "/Volumes/fraud_dev/staging/source_files/"
    "transactions/incoming/"
    + TEST_FILE_NAME
)

bad_schema_df = spark.createDataFrame(
    [
        (
            "TXN_SCHEMA_BAD_0001",
            "CUST000001",
            "2026-10-03 11:25:00",
            "1250.00",
            "INR",
            "UPI",
            "GROCERY",
            "SUCCESS",
            "DEV_SCHEMA_001",
            "10.0.0.2",
            "12.9716",
            "77.5946",
            "IN"
        )
    ],
    [
        "transaction_id",
        "customer_id",

        # merchant_id deliberately missing

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
    ]
)

# COMMAND ----------

# DBTITLE 1,Cell 19
TEMP_PATH = "/Volumes/fraud_dev/staging/source_files/transactions/_temp_bad_schema_test"

(
    bad_schema_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(TEMP_PATH)
)

# COMMAND ----------

# DBTITLE 1,Move the generated CSV to the exact incoming filename
part_file = [
    f.path
    for f in dbutils.fs.ls(TEMP_PATH)
    if f.name.startswith("part-")
][0]

dbutils.fs.cp(
    part_file,
    TEST_FILE_PATH
)

dbutils.fs.rm(
    TEMP_PATH,
    recurse=True
)

print("Created:", TEST_FILE_PATH)

# COMMAND ----------

# DBTITLE 1,Verify the test file exists:
display(
    dbutils.fs.ls(
        "/Volumes/fraud_dev/staging/source_files/"
        "transactions/incoming/"
    )
)

# COMMAND ----------

# DBTITLE 1,Failure 7
silver_after_replay = (
    spark.table(
        "fraud_dev.silver.transactions"
    )
    .count()
)

silver_distinct_after = (
    spark.table(
        "fraud_dev.silver.transactions"
    )
    .select(
        "transaction_id"
    )
    .distinct()
    .count()
)

print(
    "Silver before:",
    silver_before
)

print(
    "Silver after :",
    silver_after_replay
)

print(
    "Distinct before:",
    silver_distinct_before
)

print(
    "Distinct after :",
    silver_distinct_after
)

# COMMAND ----------

assert (
    silver_after_replay
    == silver_before
), "Silver count changed during replay"

assert (
    silver_distinct_after
    == silver_distinct_before
), "Silver business keys changed during replay"

print(
    "PASS: Silver checkpoint replay is safe."
)

# COMMAND ----------

quarantine_after = (
    spark.table(
        "fraud_dev.quarantine.transaction_rejects"
    )
    .count()
)

print(
    "Quarantine before:",
    quarantine_before
)

print(
    "Quarantine after :",
    quarantine_after
)

# COMMAND ----------

assert (
    quarantine_after
    == quarantine_before
), (
    "Quarantine rows were replayed unexpectedly"
)

print(
    "PASS: Quarantine was not duplicated."
)

# COMMAND ----------

silver_duplicate_ids = (
    spark.table(
        "fraud_dev.silver.transactions"
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
    "Silver duplicate transaction IDs:",
    silver_duplicate_ids
)

# COMMAND ----------

# DBTITLE 1,Final Failure Test 7 result
checkpoint_test_passed = (
    bronze_after_replay == bronze_before
    and
    silver_after_replay == silver_before
    and
    silver_distinct_after == silver_distinct_before
    and
    quarantine_after == quarantine_before
    and
    silver_duplicate_ids == 0
)

print(
    "FAILURE TEST 7:",
    (
        "PASS"
        if checkpoint_test_passed
        else "FAIL"
    )
)