# 🚕 NYC Yellow Taxi — Azure Databricks Data Pipeline

![Azure](https://img.shields.io/badge/Azure-Databricks-FF3621?style=flat&logo=databricks)
![Delta Lake](https://img.shields.io/badge/Delta-Lake-003366?style=flat)
![PySpark](https://img.shields.io/badge/PySpark-3.4.1-E25A1C?style=flat&logo=apache-spark)
![Python](https://img.shields.io/badge/Python-3.10-3776AB?style=flat&logo=python)

Production-grade batch data pipeline processing **38.3 million NYC Yellow Taxi trip records** using the **Medallion Architecture** on Azure Databricks. Implements real-world data engineering challenges including schema mismatch resolution, data skew handling, broadcast joins, and Delta Lake optimizations.

---

## Architecture

```
NYC TLC Data Source (CloudFront)
         ↓
    ADLS Gen2 (raw/)
    12 Parquet files — 607 MB
         ↓
  🥉 BRONZE LAYER
  Auto Loader → Delta Lake
  38,310,226 rows | 22 columns
  Schema mismatch fixed (INT64 vs INT32)
         ↓
  🥈 SILVER LAYER
  Data Quality → Broadcast Join → Enrichment
  35,574,985 rows | 34 columns
  2,735,241 invalid rows removed (7.1%)
         ↓
  🥇 GOLD LAYER
  Aggregations → Skew Handling → Z-ORDER
  3 KPI tables | Optimized for queries
         ↓
  Power BI Dashboard
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Processing Engine | Azure Databricks (Spark 3.4.1) |
| Storage | ADLS Gen2 (Azure Data Lake Storage) |
| Table Format | Delta Lake |
| Language | PySpark + Python |
| Security | Azure Key Vault + Service Principal + OAuth 2.0 |
| Orchestration | Databricks Workflows |
| Visualization | Power BI Desktop |

---

## Project Structure

```
nyc-taxi-azure-databricks-pipeline/
├── 00_mount_adls.ipynb    # ADLS authentication + data ingestion
├── 01_bronze.ipynb        # Bronze layer — raw Delta ingestion
├── 02_silver.ipynb        # Silver layer — cleaning + enrichment
├── 03_gold.ipynb          # Gold layer — KPIs + optimizations
└── README.md
```

---

## Pipeline Details

### 🔐 Security Setup
- **Service Principal** (`databricks-sp`) with `Storage Blob Data Contributor` role
- **Azure Key Vault** stores credentials (client_id, tenant_id, client_secret)
- **Databricks Secret Scope** (`kv-scope`) bridges Key Vault to notebooks
- **OAuth 2.0** authentication — no hardcoded credentials anywhere

### 🥉 Bronze Layer (`01_bronze.ipynb`)
- Ingests 12 monthly Parquet files from ADLS Gen2
- **Challenge solved:** Schema mismatch between January (INT64) and Feb-Dec (INT32) files — fixed by reading each file individually with explicit type casting
- Adds metadata columns: `ingestion_timestamp`, `source_file`, `pipeline_name`
- Writes to Delta Lake with `overwriteSchema=true`

```python
# Schema mismatch fix — explicit casting per file
df = spark.read.parquet(path)
df_cast = df.withColumn("VendorID", col("VendorID").cast("long"))
           .withColumn("PULocationID", col("PULocationID").cast("long"))
```

### 🥈 Silver Layer (`02_silver.ipynb`)
- **8 data quality rules** remove 2.7M invalid records (7.1% dirty data):
  - Negative/zero fares removed
  - Zero distance trips removed
  - Invalid passenger counts (0 or >6) removed
  - Wrong year timestamps (2008/2009 found in 2023 data) removed
  - Trips exceeding 3 hours removed
  - Duplicates removed
- **Broadcast Join** with 265-row zone lookup table:
  - Avoids shuffling 35M rows across network
  - Adds pickup/dropoff borough and zone names
- **6 derived columns** added: trip_duration_mins, pickup_hour, pickup_day, pickup_month, pickup_year, silver_pipeline
- **Partitioned** by pickup_year and pickup_month for query optimization

```python
# Broadcast join — small table sent to all executors, no shuffle
df_silver = df_cleaned.join(
    broadcast(df_zones),
    df_cleaned.PULocationID == df_zones.LocationID,
    "left"
)
```

### 🥇 Gold Layer (`03_gold.ipynb`)
- **Data Skew Handling:** Manhattan has 88% of all trips — classic skew
  - Fixed using 4-bucket salting technique
  - Distributes Manhattan partition across 4 workers
- **3 KPI tables** written to Delta Lake:
  - `gold_revenue_by_zone` — Revenue by borough and zone
  - `gold_hourly_demand` — Trip demand by hour and day
  - `gold_payment_analysis` — Payment type breakdown
- **Z-ORDER** clustering on `(pickup_borough, pickup_zone_name)` and `(pickup_hour, pickup_day)`
- **AQE** (Adaptive Query Execution) enabled for automatic optimization

```python
# Skew handling with salting
SALT_BUCKETS = 4
df_salted = df_silver.withColumn("salt", (floor(rand() * SALT_BUCKETS)).cast("int"))
# First aggregation with salt (partial results)
# Second aggregation without salt (final results)
```

---

## Key Results

| Metric | Value |
|---|---|
| Raw rows ingested | 38,310,226 |
| Clean rows after Silver | 35,574,985 |
| Dirty data removed | 2,735,241 (7.1%) |
| Top revenue zone | JFK Airport — $153M |
| Peak demand | Friday 6 PM — 411,999 trips |
| Credit card share | 81.9% of all trips |
| Total pipeline revenue | $1.03 Billion |
| Manhattan skew | 88.6% of all trips |

---

## Challenges Solved

| Challenge | Solution |
|---|---|
| Schema mismatch (INT64 vs INT32) across 12 files | Read files individually, explicit type casting |
| Auto Loader rescued 92% rows (_rescued_data) | Diagnosed root cause, switched to batch read with explicit schema |
| DBFS mounts disabled (Unity Catalog) | Direct abfss:// paths with OAuth authentication |
| RBAC permission errors on Key Vault | Step-by-step role assignment (Key Vault Secrets User) |
| Manhattan data skew (88% of trips) | 4-bucket salting distributed load across workers |
| Dirty data (wrong year timestamps) | Year filter + duration filter in Silver cleaning |
| 35M row shuffle with tiny lookup table | Broadcast join eliminated shuffle entirely |

---

## Setup Instructions

### Prerequisites
- Azure subscription (Pay-As-You-Go)
- Azure Databricks workspace (Premium tier)
- Azure Data Lake Storage Gen2
- Azure Key Vault

### Step 1 — Azure Infrastructure
```
1. Create ADLS Gen2 storage account with Hierarchical Namespace enabled
2. Create 3 containers: raw, processed, curated
3. Create Service Principal (App Registration)
4. Assign Storage Blob Data Contributor role to Service Principal
5. Create Key Vault and store 3 secrets:
   - sp-client-id
   - sp-tenant-id
   - sp-client-secret
```

### Step 2 — Databricks Setup
```
1. Create Databricks cluster (Single Node, Standard_D4ds_v4, 13.3 LTS)
2. Create Secret Scope linked to Key Vault
3. Run notebooks in order: 00 → 01 → 02 → 03
```

### Step 3 — Run Pipeline
```python
# Run notebooks in this order:
00_mount_adls.ipynb   # Setup + data ingestion
01_bronze.ipynb       # Raw ingestion to Delta
02_silver.ipynb       # Cleaning + enrichment
03_gold.ipynb         # KPIs + optimizations
```

---

## Dataset

**NYC TLC Yellow Taxi Trip Records 2023**
- Source: NYC Taxi & Limousine Commission
- Files: 12 monthly Parquet files
- Size: ~607 MB
- Rows: 38.3 million trips
- Download: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-{MM}.parquet`

---

## Author

**Alvin David**
- GitHub: [@AlvinDavid225](https://github.com/AlvinDavid225)

---

## License

MIT License — see [LICENSE](LICENSE) for details.
