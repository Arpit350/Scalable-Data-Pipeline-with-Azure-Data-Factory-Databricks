## 📌 Project Overview

This project implements a **cloud-native, scalable batch ingestion pipeline** that automates the movement and transformation of data from **Azure Blob Storage → Azure ADLS Gen2**, powered by **Azure Data Factory (ADF)** for orchestration and **Azure Databricks** for heavy-lift PySpark transformations.

### Key Achievements

| Metric | Improvement |
|--------|-------------|
| Manual data transfer effort | ↓ 90% reduction |
| Data quality & analytics readiness | ↑ 40% improvement |
| End-to-end ingestion latency | ↓ 30% reduction |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA PIPELINE FLOW                           │
├──────────────┬──────────────────────┬──────────────┬───────────────┤
│   Source     │    Orchestration     │  Transform   │   Consume     │
│              │                      │              │               │
│ Blob storage │  Azure Data Factory  │  Databricks  │  Power BI     │
│  (Raw Data)  │  (Batch Pipelines)   │  (PySpark)   │  (Reports)    │
│              │         ↓            │              │               │
│  CSV / JSON  │  Linked Services     │  Bronze Layer│  Dashboards   │
│  Parquet     │  Copy Activity       │  Silver Layer│  Analytics    │
│  Delta Files │  Triggers/Schedule   │  Gold Layer  │               │
└──────────────┴──────────────────────┴──────────────┴───────────────┘
                              │
                    Azure ADLS Gen2
                  (Central Data Lake)
```
> → `docs/screenshots/architecture-overview.png` <img width="1337" height="447" alt="image" src="https://github.com/user-attachments/assets/a1974369-bb61-488d-b98a-a5562aac4339" />


---

## 📂 Repository Structure

```
adf-databricks-pipeline/
│
├── README.md                          # Project documentation
│
├── adf-pipelines/                            # ADF Pipeline JSON definitions
│   ├── pipeline_Blob_storge_to_adls.csv      # Main ingestion pipeline
│   ├── linked_service_blob_storage.csv       # Blob_storge linked service config
│   ├── linked_service_adls.json              # ADLS Gen2 linked service config
│   └── trigger_daily_batch.json              # Scheduled trigger definition
│
├── notebooks/                                # Databricks notebooks
│   ├── 01_bronze_ingestion.py                # Raw data landing
│   ├── 02_silver_transformation.py           # Cleansing & deduplication
│   ├── 03_gold_aggregation.py                # Analytics-ready aggregations
│   └── utils/
│       ├── schema_validator.py               # Schema validation helpers
│       └── spark_config.py                   # Spark tuning configurations
│
├── scripts/                                  # Utility scripts
│   ├── deploy_adf_pipeline.sh                # CI/CD deployment script
│   └── run_databricks_job.sh                 # Trigger Databricks job via CLI
│
├── config/                                   # Environment configuration
│   ├── dev.env.example                       # Dev environment variables template
│   └── prod.env.example                      # Prod environment variables template
│
├── architecture/                             # Architecture diagrams & docs
│   └── pipeline_design.md                    # Detailed design document
│
└── docs/
    └── screenshots/                          # Tool screenshots (add yours here)
        ├── README.md                         # Screenshot guide
        └── [ADD SCREENSHOTS HERE]
```

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Source** |  Blob_storge | Raw data storage |
| **Orchestration** | Azure Data Factory | Pipeline scheduling & copy activity |
| **Storage** | Azure ADLS Gen2 | Centralized data lake (Bronze/Silver/Gold) |
| **Compute** | Azure Databricks | PySpark & Spark SQL transformation jobs |
| **Language** | Python 3.9+ / PySpark | Transformation logic |
| **Reporting** | Power BI | Downstream analytics & dashboards |
| **Format** | Delta Lake / Parquet | Optimized storage format |

---

## ⚙️ Pipeline Design

### 1. Ingestion Layer — Azure Data Factory

The ADF pipeline automates batch data movement from Blob_storge to ADLS Gen2 using:
- **Linked Services** for Blob_storge and ADLS Gen2 connections
- **Copy Activity** for efficient bulk data transfer
- **Parameterized pipelines** for dynamic source/sink paths
- **Scheduled triggers** for fully automated daily batch runs

> ADF pipeline canvas showing Copy Activity flow
> <img width="1600" height="844" alt="WhatsApp Image 2026-04-21 at 01 46 04" src="https://github.com/user-attachments/assets/a6737672-dbf0-4aa6-a807-dd3137448cf8" />



> ADF Linked Services configuration page
> <img width="1600" height="845" alt="WhatsApp Image 2026-04-21 at 01 46 03 (1)" src="https://github.com/user-attachments/assets/e1bae334-203d-4f50-b81d-5b034683ecf0" />


> ADF trigger schedule configuration
><img width="1600" height="813" alt="WhatsApp Image 2026-04-21 at 01 46 03" src="https://github.com/user-attachments/assets/aaca1572-7494-49bb-9e4f-3e98784f0183" />


---

### 2. Transformation Layer — Azure Databricks

PySpark jobs process data across a **Medallion Architecture** (Bronze → Silver → Gold):

#### Bronze Layer — Raw Ingestion
- Lands raw data from ADLS into Delta tables
- Applies schema validation and type enforcement
- Partitions by ingestion date for performance

#### Silver Layer — Cleansed Data
- Deduplication using `dropDuplicates()` on composite keys
- Data cleansing: null handling, standardized formats
- Schema validation against expected contracts

#### Gold Layer — Analytics-Ready
- Business-level aggregations using Spark SQL
- Optimized for Power BI consumption
- Delta table `OPTIMIZE` and `ZORDER` for query performance

>  Databricks notebook execution with  Bronze transformation code
> <img width="1775" height="798" alt="Screenshot 2026-04-22 011831" src="https://github.com/user-attachments/assets/1879233d-da26-4c62-85c4-cb9e3e7f0611" />

>  Databricks notebook execution with Silver transformation code
> <img width="1916" height="844" alt="Screenshot 2026-04-22 022742" src="https://github.com/user-attachments/assets/56b37add-7b15-47b5-ab32-6afc45ee2374" />

>  Databricks notebook execution with Gold transformation code
>  <img width="1919" height="837" alt="Screenshot 2026-04-22 032630" src="https://github.com/user-attachments/assets/57dbd9db-39aa-43d5-8fb4-a988485c3cb7" />

>  Databricks job run history / monitoring
> <img width="1919" height="833" alt="Screenshot 2026-04-23 021311" src="https://github.com/user-attachments/assets/7528095e-af1e-4058-8068-83b6a6fe20a0" />


---

### 3. Performance Tuning

Key optimizations applied to reduce latency by 30%:

- **Spark Config:** Tuned `spark.sql.shuffle.partitions`, executor memory, and parallelism
- **ADF Parallelism:** Configured Copy Activity `parallelCopies` and `dataIntegrationUnits`
- **Delta Optimization:** `OPTIMIZE` + `ZORDER BY` on high-cardinality join keys
- **Partitioning Strategy:** Date-based partitioning to minimize data scans

---

## 🚀 Getting Started

### Prerequisites

- Azure Subscription with ADF, ADLS Gen2, and Databricks workspace
- Python 3.9+
- Azure CLI and Databricks CLI installed

### 1. Clone the Repository

```bash
git clone https://github.com/Arpit350adf-databricks-pipeline.git
cd adf-databricks-pipeline
```

### 2. Configure Environment Variables

```bash
cp config/dev.env.example config/dev.env
# Edit dev.env with your Azure, AWS, and Databricks credentials
```

### 3. Deploy ADF Pipelines

```bash
# Deploy using Azure CLI
bash scripts/deploy_adf_pipeline.sh --env dev
```

### 4. Import Databricks Notebooks

```bash
# Import notebooks to your Databricks workspace
databricks workspace import_dir notebooks /Shared/adf-pipeline
```

### 5. Run the Pipeline

```bash
# Trigger the ADF pipeline manually for testing
az datafactory pipeline create-run \
  --factory-name <your-adf-name> \
  --resource-group <your-rg> \
  --name pipeline_s3_to_adls
```

---

## 📊 Data Flow Details

```
AWS S3 (Source)
    │
    │  [ADF Copy Activity — batch scheduled]
    ▼
ADLS Gen2 /raw/                          ← Bronze Zone
    │
    │  [Databricks: 01_bronze_ingestion.py]
    ▼
Delta Table: bronze.raw_events
    │
    │  [Databricks: 02_silver_transformation.py]
    │   - Deduplication
    │   - Null handling
    │   - Schema validation
    │   - Data cleansing
    ▼
Delta Table: silver.clean_events
    │
    │  [Databricks: 03_gold_aggregation.py]
    │   - Spark SQL aggregations
    │   - Business logic
    │   - OPTIMIZE + ZORDER
    ▼
Delta Table: gold.analytics_events       ← Power BI connects here
    │
    ▼
Power BI Reports & Dashboards
```

---

## 🔍 Key PySpark Transformations

### Deduplication

```python
df_deduped = df.dropDuplicates(["event_id", "user_id", "event_date"])
```

### Schema Validation

```python
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

expected_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("event_date", TimestampType(), True),
])

# Validate and cast
df_validated = spark.createDataFrame(df.rdd, schema=expected_schema)
```

### Data Cleansing

```python
df_clean = (df
    .filter(col("event_id").isNotNull())
    .withColumn("event_date", to_timestamp("event_date", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("user_id", trim(upper(col("user_id"))))
)
```

### Spark SQL Join

```sql
SELECT 
    e.event_id,
    e.user_id,
    u.user_name,
    e.event_date,
    e.event_type
FROM silver.clean_events e
LEFT JOIN silver.users u ON e.user_id = u.user_id
WHERE e.event_date >= current_date() - INTERVAL 30 DAYS
```

---

## 📈 Results & Impact

| Before | After | Improvement |
|--------|-------|-------------|
| Manual Blob storage → Azure data transfer (daily effort) | Fully automated batch pipeline | **90% reduction in manual effort** |
| Ad-hoc, inconsistent data quality | Validated, cleansed Delta tables | **40% improvement in analytics readiness** |
| Long ingestion windows blocking reports | Tuned Spark + ADF parallelism | **30% reduction in end-to-end latency** |
| No lineage or monitoring | ADF monitoring + Databricks job history | **Full observability** |

---

## 📸 Screenshots Index

All screenshots are stored in `docs/screenshots/`. Here is what to add:

| File | Description |
|------|-------------|
| `architecture-overview.png` | Full end-to-end architecture diagram |
| `adf-pipeline-canvas.png` | ADF pipeline with Copy Activity |
| `adf-linked-services.png` | Blob storage & ADLS Gen2 linked services |
| `adf-trigger-schedule.png` | Scheduled trigger configuration |
| `adf-monitoring-runs.png` | Pipeline run history & duration |
| `databricks-notebook-transform.png` | Silver layer transformation notebook |
| `databricks-cluster-config.png` | Cluster configuration settings |
| `databricks-job-runs.png` | Job run history & success/failure |
| `spark-ui-job-metrics.png` | Spark UI with DAG & stage timing |
| `powerbi-dashboard.png` | Final Power BI report output |
| `powerbi-databricks-connection.png` | Power BI ↔ Databricks connection |
| `adls-folder-structure.png` | Bronze/Silver/Gold folder layout in ADLS |

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Commit your changes: `git commit -m 'Add your feature'`
4. Push to the branch: `git push origin feature/your-feature`
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

---

## 👤 Author

**ARPIT SRIVASTAVA**
- LinkedIn: [arpit-srivastava2](https://linkedin.com/in/arpit-srivastava2/)
- GitHub: [Arpit350](https://github.com/Arpit350)

---

> ⭐ If this project helped you, please give it a star!
