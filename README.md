# 🍫 Sales Data Pipeline — Chocolate Retail

> An end-to-end Medallion data pipeline that transforms raw chocolate retail transactions into business intelligence — from raw CSV ingestion to Databricks Dashboards.

![Azure ADLS Gen2](https://img.shields.io/badge/Azure_ADLS_Gen2-0078D4?style=flat-square&logo=microsoft-azure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=flat-square)
![Databricks Dashboards](https://img.shields.io/badge/Databricks_Dashboards-FF3621?style=flat-square&logo=databricks&logoColor=white)
![Python](https://img.shields.io/badge/Python_3-3776AB?style=flat-square&logo=python&logoColor=white)

| Stat | Value |
|------|-------|
| Pipeline Layers | 3 (Bronze → Silver → Gold) |
| Star Schema Tables | 5 |
| Business KPIs | 20+ |
| Gold Notebooks | 4 |

---

## 1. Project Overview

This project implements a **production-grade data pipeline** for a chocolate retail business using the **Medallion Architecture** on Azure Databricks and Delta Lake. Raw transactional CSV files are ingested from Azure Data Lake Storage Gen2, progressively refined through three pipeline layers — Bronze, Silver, and Gold — and finally visualised through Databricks Dashboards.

The pipeline solves a core data engineering challenge: **how do you turn thousands of daily transactions spread across multiple source files into reliable, queryable business intelligence** — incrementally, idempotently, and at scale?

> 💡 **Key Design Principle:** Each layer is independently queryable. Bronze for raw audit, Silver for engineering work and star schema access, Gold for BI reporting with zero joins needed.

- **Incremental by Design** — Every notebook uses MERGE-based upserts. Re-run any notebook safely — results are always identical.
- **Star Schema Modeling** — Silver produces a clean star schema with surrogate keys, enabling fast analytical queries.
- **Business Ready Gold** — Gold tables answer specific business questions with no joins — plugged directly into Databricks Dashboards.

---

## 2. Business Problem

A multi-brand chocolate retailer operates **100 stores** across multiple countries, selling **200 products** from brands like Mars, Cadbury, Hershey, and Ferrero to **50,000 customers**. Without a structured data pipeline, the business faces:

| Problem Area | Challenge |
|---|---|
| **Revenue Analysis** | No visibility into monthly/yearly revenue trends, YoY growth, seasonal peaks, or AOV changes over time. |
| **Customer Insights** | Unknown loyalty member vs non-member spend behaviour, age-group distribution, and high-value customer identification. |
| **Product Performance** | No way to identify top-selling products, high revenue / low margin issues, or cocoa-tier sales patterns. |
| **Store Analysis** | Inability to compare stores against peer-group averages, detect underperformers, or split Physical vs Digital channels. |

> ✅ This pipeline answers **20+ business questions** across revenue, products, customers, and stores — fully automated, incremental, and reliable.

---

## 3. Architecture — Medallion Pattern

The pipeline follows the **Medallion Architecture**, a layered data design pattern where data quality and richness increase at each stage. Raw data enters at Bronze and emerges as business-ready aggregations at Gold.

```
🥉 BRONZE              🥈 SILVER               🥇 GOLD
Raw Ingestion     ──►  Curated Star Schema ──►  Business Analytics
─────────────         ─────────────────────    ─────────────────────
CSV/Parquet            Cleaned & typed          Pre-aggregated KPIs
Append-only            Window deduplication     Revenue, customer
Schema auto-detect     Surrogate key gen        Product, store tables
Immutable source       Star schema (dims+fact)  Databricks Dashboards
Full audit trail       MERGE incremental load   Zero joins needed
```

### End-to-End Data Flow

```
Source Files   ──►  ADLS Gen2 (Bronze Container)
                    │  sales.parquet · customers.parquet
                    │  products.parquet · stores.parquet · calendar.parquet
                    ▼
Silver Layer   ──►  Databricks Notebooks (PySpark)
                    │  dim_customers · dim_products · dim_stores
                    │  dim_calendar · fact_sales
                    ▼
Gold Layer     ──►  Aggregated Delta Tables
                    │  revenue_by_month · product_ranking
                    │  customer_analysis · store_performance
                    ▼
Serving        ──►  SQL Warehouse  ──►  Databricks Dashboards
```

---

## 4. Bronze Layer — Raw Ingestion

The Bronze layer is the **landing zone** for all raw data. Files arrive as Parquet (originally CSV converted on ingest) and are written to ADLS Gen2 with no transformations applied — preserving the exact source data for audit and reprocessing.

- **Append-Only Design** — Bronze tables only append new records. No updates, no deletes. Every historical load is preserved for full lineage tracking.
- **Schema Handling** — Schema is auto-detected on first read. `mergeSchema` enabled to handle upstream column additions without pipeline failures.
- **Source Files** — Five source files: `sales`, `customers`, `products`, `stores`, `calendar` — all partitioned by ingestion date in ADLS.

---

## 5. Silver Layer — Curation & Star Schema

The Silver layer reads from Bronze and applies a structured transformation pipeline to produce a clean, typed, deduplicated **star schema**. Every notebook follows the same pattern.

| Step | What It Does |
|---|---|
| **Data Cleaning** | Null key rows dropped. Strings trimmed and standardised. Invalid values (age > 120, discount > 1.0) set to NULL with business rule flags. |
| **Window Deduplication** | `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY date DESC)` keeps the latest record per entity. No silent data loss. |
| **Incremental MERGE** | All Silver tables use Delta `MERGE INTO` — matched rows are updated, new rows inserted. Running the notebook twice produces the same result. |
| **Surrogate Keys** | Integer surrogate keys generated via `ROW_NUMBER()`. Existing SKs are reused on incremental runs — new entities get `max_sk + N`. |

---

## 6. Star Schema Design

The Silver layer produces a **classic star schema** with four dimension tables surrounding a central fact table. All joins use integer surrogate keys for maximum query performance.

```
                    ┌──────────────────┐
                    │  dim_customers   │
                    │  customer_sk PK  │
                    │  customer_id     │
                    │  age · age_band  │
                    │  gender          │
                    │  loyalty_label   │
                    └────────┬─────────┘
                             │
┌──────────────┐    ┌────────▼──────────────────┐    ┌──────────────────┐
│ dim_calendar │    │       fact_sales           │    │  dim_products    │
│  date_sk PK  │◄───│  date_sk FK               │───►│  product_sk PK   │
│  date        │    │  customer_sk FK            │    │  product_id      │
│  year/quarter│    │  product_sk FK             │    │  brand · category│
│  month_name  │    │  store_sk FK               │    │  cocoa_tier      │
│  is_weekend  │    │  ─────────────────         │    │  weight_band     │
└──────────────┘    │  order_id · order_date     │    └──────────────────┘
                    │  revenue · cost · profit   │
                    │  quantity · discount        │    ┌──────────────────┐
                    │  profit_margin · is_valid  │───►│  dim_stores      │
                    └────────────────────────────┘    │  store_sk PK     │
                                                       │  store_id        │
                                                       │  city · country  │
                                                       │  store_type      │
                                                       │  channel         │
                                                       └──────────────────┘
```

| Table | Type | PK / Join Key | Key Derived Columns | Rows |
|---|---|---|---|---|
| `dim_customers` | Dimension | `customer_sk` | age_band, loyalty_label | ~50,000 |
| `dim_products` | Dimension | `product_sk` | cocoa_tier, weight_band | 200 |
| `dim_stores` | Dimension | `store_sk` | channel (Physical/Digital) | 100 |
| `dim_calendar` | Dimension | `date_sk` (YYYYMMDD) | quarter, month_name, is_weekend | 731 |
| `fact_sales` | Fact | 4 × surrogate FKs | profit_margin, gross_sales, is_valid | Large |

> ℹ️ `dim_calendar` uses a **YYYYMMDD integer** as its surrogate key (e.g. `20230715`). Integer equality joins are faster than date equality, and the key is self-documenting.

---

## 7. Slowly Changing Dimensions (SCD Type 1)

All dimension tables implement **SCD Type 1** — when a dimension attribute changes (e.g. a customer's loyalty status, a store's city), the old value is **overwritten** with the new value. No history is kept for Type 1 attributes.

This is implemented via the Delta `MERGE INTO` pattern — the `WHEN MATCHED THEN UPDATE SET` clause overwrites changed attributes while the surrogate key remains stable.

> ⚠️ **SCD Type 1 trade-off:** If a customer upgrades to a loyalty member, historical orders will appear as if they were always a loyalty member. For historical accuracy, upgrade to SCD Type 2 (see Future Enhancements).

---

## 8. Gold Layer — Business Analytics

The Gold layer reads from the Silver star schema and produces **pre-aggregated analytical tables**. Each table answers specific business questions — Databricks Dashboards connect directly to Gold with no further transformation required.

| Notebook | Gold Tables | Business Questions Answered |
|---|---|---|
| `gold_revenue_sales` | `revenue_by_month`, `yoy_growth`, `aov_summary`, `daily_sales_trend`, `sales_by_day_type` | Monthly revenue & profit · YoY growth · Average Order Value · Daily trends · Weekday vs weekend |
| `gold_product_analysis` | `product_revenue_ranking`, `top10_products`, `category_performance`, `margin_issue_products`, `cocoa_sales_analysis` | Product rankings · Top 10 by units · Category revenue share · High revenue/low margin alerts · Cocoa % vs sales |
| `gold_customer_analysis` | `customer_revenue_ranking`, `revenue_by_age_band`, `loyalty_vs_nonloyalty`, `customer_spend_summary`, `orders_per_customer` | Top customers by revenue · Age-band spend · Loyalty member impact · Avg spend & percentiles · Order frequency |
| `gold_store_performance` | `store_revenue_ranking`, `city_country_performance`, `store_type_performance`, `underperforming_stores`, `channel_comparison` | Store rankings · City/country best performers · Mall vs Retail vs Online · Underperformers vs peer avg · Physical vs Digital |

---

## 9. Databricks Dashboards

All dashboards are built natively in **Databricks Dashboards**, querying the Gold Delta tables directly via SQL Warehouse.

### Revenue & Sales Dashboard

![YoY Growth](https://github.com/user-attachments/assets/ea018116-98e4-4a65-b518-8b6530a0806b)

![Sales by Day Type](https://github.com/user-attachments/assets/c3b8c260-98e9-475f-ae7b-0c90f6a41f37)

![Monthly Revenue Trend](https://github.com/user-attachments/assets/dad5c59c-51bc-4266-ad29-f28630499b1b)

Key visuals: Monthly revenue & profit trend · YoY growth % · Average Order Value · Daily sales trend 

---

##  Product Analysis Dashboard

![Top 10 Products by Revenue](https://github.com/user-attachments/assets/16c3283b-671e-45f1-ab0d-78df01e4f716)

![Category Performance](https://github.com/user-attachments/assets/d4617f7b-da98-4bc0-bb99-bb67da243f92)

![Margin Issue Products](https://github.com/user-attachments/assets/12769f2e-7184-4814-855a-1a7331647c84)

Key visuals: Product revenue ranking · Top 10 products by units sold · Category revenue share · High revenue / low margin alerts · Cocoa % vs sales

---

##  Customer Analysis Dashboard

![Revenue by Age Band](https://github.com/user-attachments/assets/acdbfde9-c473-4f65-9f77-c77161e5432d)

![Loyalty vs Non-Loyalty](https://github.com/user-attachments/assets/14a89e40-d4cc-4d14-935a-1293ac3f3913)

![Customer Frequency Segments](https://github.com/user-attachments/assets/71013eaf-6ed8-4694-9f09-92625ecce888)

Key visuals: Top customers by revenue · Revenue by age band · Loyalty vs non-loyalty member spend · Customer spend percentiles · Order frequency distribution

---

##  Store Performance Dashboard

![Top 10 Stores by Revenue](https://github.com/user-attachments/assets/f8623a72-7a6e-4b30-95ae-55949d2d9e89)

![Revenue by Store Type](https://github.com/user-attachments/assets/5583bbae-da8f-4bc3-b06b-57cbd9d1a658)

![Channel Revenue Trend](https://github.com/user-attachments/assets/09d250a6-1bda-4b9c-a27e-4804a58287d0)

![Underperforming Stores](https://github.com/user-attachments/assets/89aae909-8125-40a5-a622-0f26c9d28cae)

Key visuals: Store revenue ranking · City & country best performers · Mall vs Retail vs Online store type · Underperforming stores vs peer average · Physical vs Digital channel comparison

---

## 10. Data Pipeline Flow

```
01 📥  Raw File Landing          [Bronze]
       CSV source files land in ADLS Gen2 Bronze container.
       Auto Loader appends to Bronze Delta tables with
       ingestion timestamp and source file metadata.
        │
02 🧹  Dimension Cleaning        [Silver — run in parallel]
       dim_calendar · dim_customers · dim_products · dim_stores
       Each reads Bronze → cleans → assigns surrogate keys
       → MERGEs into Silver.
        │
03 🔗  Fact Table Construction   [Silver — depends on dims]
       fact_sales reads Bronze sales, validates business rules
       (is_valid flag), LEFT JOINs all four dims to resolve
       surrogate FKs, MERGEs into silver.fact_sales.
       Partitioned by order_year.
        │
04 📊  Gold Aggregations         [Gold — run in parallel]
       All four Gold notebooks run after fact_sales is ready.
       Reads from Silver star schema, writes pre-aggregated
       KPI tables to gold database. Overwrite strategy.
        │
05 🗄  SQL Warehouse Serving     [Serving Layer]
       Databricks SQL Warehouse exposes Gold Delta tables
       as SQL endpoints. Handles concurrency and
       auto-scaling independently from Spark clusters.
        │
06 📈  Databricks Dashboard Refresh
       Dashboards query Gold tables directly via SQL Warehouse.
       Auto-refresh on schedule after Gold pipeline completes.
```

---

## 11. Technologies Used

| Technology | Role |
|---|---|
| ☁ **Azure ADLS Gen2** | Cloud storage — all layers |
| ⚡ **Databricks** | Unified analytics platform |
| 🔷 **PySpark** | Distributed data processing |
| △ **Delta Lake** | ACID transactions, time travel |
| 🗄 **SQL Warehouse** | Serverless query serving layer |
| 📊 **Databricks Dashboards** | Business dashboards |
| 🐍 **Python 3** | Pipeline orchestration |
| 📐 **Spark SQL** | Gold aggregations |

---

## 12. Key Features

- 🔁 Incremental Ingestion
- 🔀 Merge / Upsert Logic
- ⭐ Star Schema Modeling
- 🔑 Surrogate Key Generation
- 🪪 SCD Type 1
- 📊 20+ KPI Gold Tables
- 🪟 Window Function Deduplication
- 🗄 SQL Warehouse Serving
- 🛡 Data Quality Flags (`is_valid`)
- 📅 Enriched Date Dimension
- 🔒 Idempotent Pipelines
- 🔬 Partition + Z-ORDER Optimization

---

## 13. Project Structure

```
chocolate-retail-dw/
│
├── Silver Layer/                  # Star schema creation notebooks
│   ├── dim_customers.py           # Customer dim — SK, age_band, loyalty_label
│   ├── dim_products.py            # Product dim  — SK, cocoa_tier, weight_band
│   ├── dim_stores.py              # Store dim    — SK, channel classification
│   ├── dim_calendar.py            # Date dim     — YYYYMMDD SK, full attributes
│   └── fact_sales.py              # Fact table   — all FK surrogate keys
│
├── Gold Layer/                    # Business analytics notebooks
│   ├── gold_revenue_sales.py      # Monthly revenue, AOV, YoY growth, day trends
│   ├── gold_product_analysis.py   # Rankings, categories, margin alerts, cocoa
│   ├── gold_customer_analysis.py  # Tiers, loyalty, age-band, RFM
│   └── gold_store_performance.py  # Geo, underperformers, channel comparison
│
├── Datasets/                      # Source sample data (CSV)
│   ├── sales.csv
│   ├── customers.csv
│   ├── products.csv
│   ├── stores.csv
│   └── calendar.csv
│
├── dashboards/                    # Databricks Dashboard screenshots
│   ├── revenue_sales.png
│   ├── product_analysis.png
│   ├── customer_analysis.png
│   └── store_performance.png
│
└── README.md
```

---

## 14. How to Run

### Step 1 — Set up Azure Resources

Create an Azure Data Lake Storage Gen2 account. Create three containers: `bronze`, `silver`, `gold`. Upload the five source CSV files to the `bronze` container.

### Step 2 — Configure Databricks Workspace

Create a Databricks workspace in Azure. Set up a cluster with Spark 3.x and Delta Lake runtime. Mount the ADLS containers using a service principal or managed identity. Update `STORAGE_ACCOUNT` in each notebook.

### Step 3 — Run Silver Dimension Notebooks *(in parallel)*

Dimensions can run simultaneously; the fact table must wait for all dims to complete.

```
dim_calendar.py  ──┐
dim_customers.py ──┤──► (all in parallel)
dim_products.py  ──┤
dim_stores.py    ──┘
```

> On **first run**: uncomment the `overwrite` block in each notebook to create the Delta table, then comment it back out for all subsequent runs.

### Step 4 — Run `fact_sales.py`

After all four dimension notebooks complete, run `fact_sales.py`. This notebook JOINs all dims to resolve surrogate keys.

> First run: uncomment the `overwrite` block to create the partitioned Delta table.

### Step 5 — Run Gold Notebooks *(in parallel)*

All four Gold notebooks can run simultaneously after `fact_sales` is ready.

```
gold_revenue_sales.py      ──┐
gold_product_analysis.py   ──┤──► (all in parallel)
gold_customer_analysis.py  ──┤
gold_store_performance.py  ──┘
```

### Step 6 — View Databricks Dashboards

Open the Databricks workspace and navigate to the Dashboards section. Dashboards query Gold tables directly via SQL Warehouse and auto-refresh after each pipeline run.

> ♻️ **Incremental runs:** After first-time setup, every subsequent run is fully incremental. MERGE handles inserts and updates automatically — no data will be duplicated.


*Medallion Architecture · Azure Databricks · Delta Lake · Star Schema · Databricks Dashboards*
