# Sample Test Datasets

This directory provides realistic, schema-validated test datasets to test the end-to-end pipeline in a Databricks workspace or sandbox environment.

## File Manifest & Schema Alignment

| File Name | Intended Volume Path | Description |
|-----------|----------------------|-------------|
| `customers_20261002.csv` | `/Volumes/fraud_dev/staging/source_files/customers/incoming/` | Baseline customer demographics, risk ratings, and average spending habits. |
| `merchants_20261002.csv` | `/Volumes/fraud_dev/staging/source_files/merchants/incoming/` | Merchant profiles, business categories, historical fraud rates, and chargeback rates. |
| `fraud_labels_20261002.csv` | `/Volumes/fraud_dev/staging/source_files/fraud_labels/incoming/` | Historical labelled dataset used for training the MLflow fraud detection model. |
| `transactions_20261002_140100_001.csv` | `/Volumes/fraud_dev/staging/source_files/transactions/incoming/` | Real-time streaming transaction batch ingested via Databricks Auto Loader (`cloudFiles`). |

## Uploading Test Data to Unity Catalog Volumes

You can upload these files using the Databricks UI or CLI:

```bash
# Example upload using Databricks CLI
databricks fs cp sample_data/customers_20261002.csv \
  dbfs:/Volumes/fraud_dev/staging/source_files/customers/incoming/customers_20261002.csv

databricks fs cp sample_data/merchants_20261002.csv \
  dbfs:/Volumes/fraud_dev/staging/source_files/merchants/incoming/merchants_20261002.csv

databricks fs cp sample_data/fraud_labels_20261002.csv \
  dbfs:/Volumes/fraud_dev/staging/source_files/fraud_labels/incoming/fraud_labels_20261002.csv

databricks fs cp sample_data/transactions_20261002_140100_001.csv \
  dbfs:/Volumes/fraud_dev/staging/source_files/transactions/incoming/transactions_20261002_140100_001.csv
```
