# 🛒 E-Commerce Sales Analytics Pipeline
### Azure Data Engineering Project | Medallion Architecture

---

## 📌 Project Overview

An end-to-end cloud data pipeline built on Microsoft Azure that ingests, transforms, and analyzes the **Olist Brazilian E-Commerce dataset** (100,000+ orders across 9 CSV files). The pipeline follows **Medallion Architecture (Bronze → Silver → Gold)** and is fully orchestrated via **Azure Data Factory**.

**Key business insights surfaced in the Gold layer:**
- 📈 Peak revenue month: **November 2017 — $1.19M** (Black Friday effect)
- 🏆 Top product category: **Health & Beauty — $1.25M revenue**
- 🚚 Delivery performance tracked across **27 Brazilian states**
- 💳 **Credit card dominates** at 73%+ of all transactions

---

## 🏗️ Architecture

```
Kaggle Dataset (9 CSVs)
        │
        ▼
┌─────────────────────┐
│   Azure Data Lake   │  ← Raw CSV files landed in ecommerce-b/raw/
│   Storage Gen2      │
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│  Azure Data Factory │  ← Parameterized ForEach pipeline
│  ecommerce_pipeline │     dynamically ingests all 9 files
└─────────────────────┘
        │
        ▼
┌──────────────────────────────────────────────────┐
│              Azure Databricks                    │
│                                                  │
│  🥉 Bronze Layer (ecommerce_bronze.sales)        │
│     Raw data ingested as-is into Delta tables    │
│                                                  │
│  🥈 Silver Layer (ecommerce_silver.sales)        │
│     Type casting, deduplication, null handling   │
│     Multi-table joins, timestamp conversion      │
│                                                  │
│  🥇 Gold Layer (ecommerce_gold.sales)            │
│     Business aggregations, KPIs, analytics       │
└──────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────┐
│   Unity Catalog     │  ← All layers governed via Unity Catalog
│   Delta Lake        │     Delta format with ACID guarantees
└─────────────────────┘
```

---

## 📦 Dataset

**Source:** [Olist Brazilian E-Commerce — Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

| File | Description | Rows |
|------|-------------|------|
| olist_orders_dataset.csv | Order header info | 99,441 |
| olist_order_items_dataset.csv | Line items per order | 112,650 |
| olist_order_payments_dataset.csv | Payment details | 103,886 |
| olist_order_reviews_dataset.csv | Customer reviews | 99,224 |
| olist_customers_dataset.csv | Customer profiles | 99,441 |
| olist_products_dataset.csv | Product catalogue | 32,951 |
| olist_sellers_dataset.csv | Seller profiles | 3,095 |
| olist_geolocation_dataset.csv | ZIP code coordinates | 1,000,163 |
| product_category_name_translation.csv | Category translations | 71 |

---

## ⚙️ Tech Stack

| Layer | Technology |
|-------|------------|
| Cloud Platform | Microsoft Azure |
| Storage | Azure Data Lake Storage Gen2 |
| Orchestration | Azure Data Factory (Parameterized ForEach Pipeline) |
| Processing | Azure Databricks (PySpark) |
| File Format | Delta Lake (Parquet + transaction log) |
| Governance | Unity Catalog |
| Language | Python, PySpark, Spark SQL |
| Version Control | GitHub |

---

## 🔄 ADF Pipeline Design

The pipeline uses a **parameterized ForEach loop** — instead of 9 hardcoded Copy activities, a single dynamic Copy activity handles all files using `@{concat(item(), '.csv')}` expressions.

```
[ForEach Copy Files] → [Bronze Notebook] → [Silver Notebook] → [Gold Notebook]
```

**Pipeline parameter:**
```json
file_list: [
  "olist_orders_dataset",
  "olist_order_items_dataset",
  "olist_order_payments_dataset",
  "olist_order_reviews_dataset",
  "olist_customers_dataset",
  "olist_products_dataset",
  "olist_sellers_dataset",
  "olist_geolocation_dataset",
  "product_category_name_translation"
]
```

---

## 🥉 Bronze Layer

**Notebook:** `01_bronze_ecommerce`

- Reads all 9 CSVs from ADLS Gen2 using `abfss://` protocol
- Validates row counts per file
- Writes raw data as Delta tables to `ecommerce_bronze.sales`

**Tables created:**
```
ecommerce_bronze.sales.orders_raw
ecommerce_bronze.sales.order_items_raw
ecommerce_bronze.sales.payments_raw
ecommerce_bronze.sales.reviews_raw
ecommerce_bronze.sales.customers_raw
ecommerce_bronze.sales.products_raw
ecommerce_bronze.sales.sellers_raw
ecommerce_bronze.sales.geolocation_raw
ecommerce_bronze.sales.category_raw
```

---

## 🥈 Silver Layer

**Notebook:** `02_silver_ecommerce`

Transformations applied per table:

| Table | Transformations |
|-------|----------------|
| orders | Timestamp casting (5 date columns), null filter, dedup on order_id |
| order_items | Cast price & freight to double, filter price > 0 |
| payments | Cast payment_value to double, installments to int |
| customers | Trim state/city, cast zip code to int, dedup on customer_id |
| products | Join with category translation (left join), cast dimensions to double |
| sellers | Trim state/city, cast zip code, dedup on seller_id |

**Tables created:**
```
ecommerce_silver.sales.orders
ecommerce_silver.sales.order_items
ecommerce_silver.sales.payments
ecommerce_silver.sales.customers
ecommerce_silver.sales.products
ecommerce_silver.sales.sellers
```

---

## 🥇 Gold Layer

**Notebook:** `03_gold_ecommerce`

### Monthly Revenue Trend (25 rows)
```
Year | Month | Total Revenue  | Total Orders | Avg Order Value
2017 | 11    | $1,194,882.80  | 7,863        | $151.96   ← Peak (Black Friday)
2017 | 10    | $779,677.88    | 4,859        | $160.46
2017 | 09    | $727,762.45    | 4,516        | $161.15
```

### Top Product Categories by Revenue (71 categories)
```
Category              | Total Revenue  | Total Orders | Avg Price
health_beauty         | $1,258,681.34  | 9,670        | $130.16   ← #1
watches_gifts         | $1,205,005.68  | 5,991        | $201.14   ← Highest avg price
bed_bath_table        | $1,036,988.68  | 11,115       | $93.30    ← Most orders
sports_leisure        | $988,048.97    | 8,641        | $114.34
computers_accessories | $911,954.32    | 7,827        | $116.51
```

### Delivery Performance (27 states)
- Tracks average delivery days and late delivery % per Brazilian state

### Payment Methods (4 types)
- Credit card, boleto, voucher, debit card breakdown by transaction count and value

### Top Sellers (3,095 sellers)
- Top seller from Guariba, SP: **$229,472 revenue**

---

## 📁 Repository Structure

```
Ecommerce_Sales_Analytics/
├── notebooks/
│   ├── 01_bronze_ecommerce.ipynb
│   ├── 02_silver_ecommerce.ipynb
│   └── 03_gold_ecommerce.ipynb
└── README.md
```

---

## 🚀 How to Run

1. Upload the 9 Olist CSVs to `ADLS Gen2 container/raw/`
2. Set your storage account key in each notebook (Cell 1)
3. Trigger `ecommerce_pipeline` in Azure Data Factory
4. Pipeline automatically runs Bronze → Silver → Gold in sequence
5. Query gold tables via Databricks SQL Warehouse or Catalog Explorer

---

## 🔗 Related Projects

- [IPL Cricket Analytics Pipeline](../IPL_Cricket_Analytics/)
