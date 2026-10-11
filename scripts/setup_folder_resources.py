# Databricks notebook source
# ==============================================================================
# Setup Volume Directories & Landing Zones
# Real-Time Transaction Fraud & Anomaly Detection Platform
# ==============================================================================

base = "/Volumes/fraud_dev/staging/source_files"

folders = [
    # Transactions streaming landing zones
    "transactions/incoming",
    "transactions/rejected",
    "transactions/archive",

    # Customers reference landing zones
    "customers/incoming",
    "customers/rejected",
    "customers/archive",

    # Merchants reference landing zones
    "merchants/incoming",
    "merchants/rejected",
    "merchants/archive",

    # Fraud labels landing zones
    "fraud_labels/incoming",
    "fraud_labels/rejected",
    "fraud_labels/archive",

    # Streaming checkpoints
    "checkpoints/bronze_transactions",
    "checkpoints/bronze_transactions_schema",
    "checkpoints/silver_transactions_v2",
    "checkpoints/fraud_processing"
]

for folder in folders:
    path = f"{base}/{folder}"
    try:
        dbutils.fs.mkdirs(path)
        print(f"✅ Successfully created: {path}")
    except Exception as e:
        print(f"⚠️ Error creating {path}: {str(e)}")

print("\n🎉 Volume directory initialization complete.")
