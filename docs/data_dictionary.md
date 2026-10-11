# Data Dictionary & Schema Specifications

This document outlines the schemas, types, and constraints across all Unity Catalog schemas in the `fraud_dev` catalog.

---

## 1. Bronze Layer Tables (`fraud_dev.bronze`)

### `fraud_dev.bronze.transactions`
| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `transaction_id` | STRING | No | Unique transaction identifier |
| `customer_id` | STRING | Yes | Customer identifier foreign key |
| `merchant_id` | STRING | Yes | Merchant identifier foreign key |
| `transaction_timestamp` | STRING | Yes | Timestamp of transaction occurrence (ISO format) |
| `amount` | STRING | Yes | Transaction transaction value (raw string) |
| `currency_code` | STRING | Yes | Currency code (e.g. USD, EUR) |
| `payment_method` | STRING | Yes | Card type, Apple Pay, Online Banking, etc. |
| `merchant_category` | STRING | Yes | Merchant industry category |
| `transaction_status` | STRING | Yes | Status at authorization |
| `device_id` | STRING | Yes | Originating device fingerprint |
| `ip_address` | STRING | Yes | Client IP address |
| `latitude` | STRING | Yes | Geographic location latitude |
| `longitude` | STRING | Yes | Geographic location longitude |
| `country_code` | STRING | Yes | Country code of terminal |
| `_metadata` | STRUCT | Yes | Auto Loader file metadata |

### `fraud_dev.bronze.customers`
| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `customer_id` | STRING | No | Unique customer identifier |
| `customer_name` | STRING | Yes | Customer full name |
| `date_of_birth` | STRING | Yes | Customer birth date |
| `customer_since` | STRING | Yes | Account creation date |
| `customer_status` | STRING | Yes | ACTIVE, SUSPENDED, CLOSED |
| `home_country` | STRING | Yes | Country of residence |
| `home_state` | STRING | Yes | State/Province |
| `home_city` | STRING | Yes | City |
| `preferred_currency` | STRING | Yes | Account base currency |
| `customer_risk_level` | STRING | Yes | LOW, MEDIUM, HIGH |
| `avg_monthly_spend` | STRING | Yes | Historical monthly spend |
| `avg_transaction_amount` | STRING | Yes | Average per-transaction spend |
| `created_at` | STRING | Yes | Ingestion timestamp |
| `updated_at` | STRING | Yes | Last update timestamp |

### `fraud_dev.bronze.merchants`
| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `merchant_id` | STRING | No | Unique merchant ID |
| `merchant_name` | STRING | Yes | Legal business name |
| `merchant_category` | STRING | Yes | Merchant classification |
| `merchant_country` | STRING | Yes | Merchant country |
| `merchant_state` | STRING | Yes | Merchant state |
| `merchant_city` | STRING | Yes | Merchant city |
| `merchant_status` | STRING | Yes | ACTIVE, BLOCKED |
| `merchant_risk_level` | STRING | Yes | LOW, MEDIUM, HIGH |
| `chargeback_rate` | STRING | Yes | Historical chargeback ratio |
| `fraud_rate` | STRING | Yes | Historical confirmed fraud ratio |
| `created_at` | STRING | Yes | Ingestion timestamp |
| `updated_at` | STRING | Yes | Modification timestamp |

---

## 2. Silver Layer Tables (`fraud_dev.silver`)

### `fraud_dev.silver.transactions`
| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `transaction_id` | STRING | Unique transaction identifier |
| `customer_id` | STRING | Customer ID |
| `merchant_id` | STRING | Merchant ID |
| `transaction_timestamp` | TIMESTAMP | Cast timestamp |
| `amount` | DOUBLE | Validated numerical amount |
| `currency_code` | STRING | Standardized currency |
| `payment_method` | STRING | Standardized payment channel |
| `merchant_category` | STRING | Standardized category |
| `transaction_status` | STRING | Authorization status |
| `device_id` | STRING | Device identifier |
| `ip_address` | STRING | IP address |
| `latitude` | DOUBLE | Numerical latitude |
| `longitude` | DOUBLE | Numerical longitude |
| `country_code` | STRING | Standard 2-letter country code |

### `fraud_dev.silver.transaction_complete_fraud_features`
| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `transaction_id` | STRING | Primary transaction identifier |
| `customer_id` | STRING | Customer identifier |
| `merchant_id` | STRING | Merchant identifier |
| `amount` | DOUBLE | Transaction amount |
| `txn_count_5m` | BIGINT | Rolling 5-minute transaction count for customer |
| `txn_total_amount_5m` | DOUBLE | Rolling 5-minute cumulative spend |
| `txn_count_10m` | BIGINT | Rolling 10-minute transaction count for customer |
| `txn_total_amount_10m` | DOUBLE | Rolling 10-minute cumulative spend |
| `amount_vs_customer_avg` | DOUBLE | Ratio of amount to customer average spend |
| `customer_risk_level` | STRING | Customer risk tier |
| `merchant_risk_level` | STRING | Merchant risk tier |
| `fraud_rate` | DOUBLE | Merchant historical fraud rate |
| `chargeback_rate` | DOUBLE | Merchant historical chargeback rate |
| `rule_high_amount` | INTEGER | High-amount deterministic rule trigger (1 or 0) |

---

## 3. Gold Layer Tables (`fraud_dev.gold`)

### `fraud_dev.gold.fraud_transaction_decisions`
| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `transaction_id` | STRING | Transaction ID |
| `customer_id` | STRING | Customer ID |
| `merchant_id` | STRING | Merchant ID |
| `transaction_timestamp` | TIMESTAMP | Event timestamp |
| `amount` | DOUBLE | Amount |
| `fraud_probability` | DOUBLE | ML model predicted probability (0.0 to 1.0) |
| `decision_threshold` | DOUBLE | Active model cutoff threshold |
| `rule_high_amount` | INTEGER | Deterministic high-amount rule flag |
| `decision` | STRING | Final decision: `BLOCK`, `REVIEW`, `ALLOW` |
| `active_model_version`| STRING | Model version used for inference |
| `processed_at` | TIMESTAMP | Scoring timestamp |

---

## 4. Control & Governance Tables (`fraud_dev.control`)

### `fraud_dev.control.file_processing_status`
Tracks file discovery, validation, processing status (`READY`, `PROCESSED`, `REJECTED`, `ARCHIVED`), and error reasons.

### `fraud_dev.control.pipeline_runs`
Maintains batch/streaming execution metadata, start/end timestamps, failure layers, and recovery state.

### `fraud_dev.control.reconciliation_results`
Stores automated row balance checks across pipeline stages:
$$\text{source\_count} = \text{target\_count} + \text{quarantine\_count} + \text{duplicate\_count}$$

### `fraud_dev.control.model_status`
Manages MLflow model promotions, version strings, active status (`is_active = true`), PR-AUC validation metrics, and decision thresholds.
