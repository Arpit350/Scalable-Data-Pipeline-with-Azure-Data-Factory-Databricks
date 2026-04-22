# =============================================================================
# silver/silver_transformation.py
# Silver Layer — Cleansed & Conformed Data (SQL-Based Transformations)
#
# Responsibilities:
#   1. Read from Bronze Delta table
#   2. Deduplicate on composite keys (DEST_COUNTRY_NAME + ORIGIN_COUNTRY_NAME)
#   3. Null handling and data quality enforcement
#   4. Standardize formats (country names, uppercase)
#   5. Type casting to final Silver schema
#   6. MERGE (SCD Type 1 upsert) into Silver Delta table
# =============================================================================
# =============================================================================
# Silver Layer Transformation (Pure PySpark DataFrame API)
# Cleansed & Conformed Data
#
# Responsibilities:
#   1. Read from Bronze Delta table
#   2. Deduplicate on composite keys using Window functions
#   3. Null handling and data quality enforcement
#   4. Standardize formats (country names, uppercase)
#   5. Type casting to final Silver schema
#   6. MERGE into Silver Delta table using DeltaTable API
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, upper, trim, coalesce, lit, current_timestamp, current_date,
    year, month, row_number, count, sum as spark_sum, countDistinct, when
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import date

print("=== Silver Layer Transformation (PySpark DataFrame API) ===")

# Configuration
CATALOG = "databricks_dlt"
SCHEMA = "sample_dlt_table"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_orders"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_country_flights"

# ─── 1. Create Silver Table ─────────────────────────────────────────────────
def create_silver_table() -> None:
    """Create Silver table if it does not exist."""
    print("\n1. Creating Silver table (if not exists)...")
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            dest_country STRING NOT NULL,
            origin_country STRING NOT NULL,
            flight_count INT,
            ingestion_year INT,
            ingestion_month INT,
            _silver_processed_at TIMESTAMP,
            _source_batch_id STRING
        )
        USING DELTA
        PARTITIONED BY (ingestion_year, ingestion_month)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true',
            'delta.enableChangeDataFeed' = 'true'
        )
    """)
    print("   ✓ Silver table ready")


# ─── 2. Transform Bronze to Silver ──────────────────────────────────────────
def transform_bronze_to_silver(partition_dt: str):
    """Read Bronze, deduplicate, standardize, and prepare for merge."""
    print(f"\n2. Transforming Bronze data (partition: {partition_dt})...")
    
    # Read Bronze partition
    bronze_df = spark.table(BRONZE_TABLE).filter(col("_partition_dt") == partition_dt)
    
    print(f"   • Bronze records read: {bronze_df.count()}")
    
    # Deduplicate using Window function (keep latest by _ingested_at)
    window_spec = Window.partitionBy("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").orderBy(col("_ingested_at").desc())
    
    bronze_ranked = bronze_df.withColumn("_rn", row_number().over(window_spec))
    
    # Filter: keep only rank 1 (most recent) and non-null keys
    deduped_df = bronze_ranked.filter(
        (col("_rn") == 1) &
        col("DEST_COUNTRY_NAME").isNotNull() &
        col("ORIGIN_COUNTRY_NAME").isNotNull()
    )
    
    print(f"   • After deduplication: {deduped_df.count()} records")
    
    # Standardize: uppercase, trim, coalesce nulls
    standardized_df = deduped_df.select(
        upper(trim(col("DEST_COUNTRY_NAME"))).alias("dest_country"),
        upper(trim(col("ORIGIN_COUNTRY_NAME"))).alias("origin_country"),
        coalesce(col("count"), lit(0)).cast("int").alias("flight_count"),
        col("_batch_id"),
        col("_ingested_at")
    )
    
    # Final transformation: add ingestion metadata
    final_df = standardized_df.select(
        col("dest_country"),
        col("origin_country"),
        col("flight_count"),
        year(col("_ingested_at")).alias("ingestion_year"),
        month(col("_ingested_at")).alias("ingestion_month"),
        current_timestamp().alias("_silver_processed_at"),
        col("_batch_id").cast("string").alias("_source_batch_id")
    )
    
    print(f"   ✓ Transformation complete: {final_df.count()} records ready")
    return final_df


# ─── 3. Merge into Silver (SCD Type 1) ──────────────────────────────────────
def merge_to_silver(source_df):
    """Merge source DataFrame into Silver table using Delta merge."""
    print("\n3. Merging into Silver table...")
    
    # Get Delta table
    silver_delta = DeltaTable.forName(spark, SILVER_TABLE)
    
    # Merge condition: match on country pair + ingestion period
    merge_condition = """
        target.dest_country = source.dest_country AND
        target.origin_country = source.origin_country AND
        target.ingestion_year = source.ingestion_year AND
        target.ingestion_month = source.ingestion_month
    """
    
    # Execute merge (SCD Type 1: update existing, insert new)
    silver_delta.alias("target").merge(
        source_df.alias("source"),
        merge_condition
    ).whenMatchedUpdate(set={
        "flight_count": "source.flight_count",
        "_silver_processed_at": "source._silver_processed_at",
        "_source_batch_id": "source._source_batch_id"
    }).whenNotMatchedInsert(values={
        "dest_country": "source.dest_country",
        "origin_country": "source.origin_country",
        "flight_count": "source.flight_count",
        "ingestion_year": "source.ingestion_year",
        "ingestion_month": "source.ingestion_month",
        "_silver_processed_at": "source._silver_processed_at",
        "_source_batch_id": "source._source_batch_id"
    }).execute()
    
    print("   ✓ Merge complete")


# ─── 4. Validation ──────────────────────────────────────────────────────────
def validate_silver():
    """Run data quality checks on Silver table."""
    print("\n4. Running validation...")
    
    silver_df = spark.table(SILVER_TABLE).filter(
        (col("ingestion_year") == year(current_date())) &
        (col("ingestion_month") == month(current_date()))
    )
    
    validation_stats = silver_df.agg(
        lit(str(date.today())).alias("transformation_date"),
        count("*").alias("total_rows"),
        countDistinct("dest_country").alias("unique_destinations"),
        countDistinct("origin_country").alias("unique_origins"),
        spark_sum("flight_count").alias("total_flights"),
        spark_sum(when(col("flight_count") == 0, 1).otherwise(0)).alias("zero_flights"),
        spark_sum(when(col("flight_count") < 0, 1).otherwise(0)).alias("negative_flights")
    )
    
    print("   • Validation Statistics:")
    display(validation_stats)
    
    # Data quality check
    quality_check = silver_df.filter(col("flight_count") < 0).agg(
        lit("Negative Flight Count").alias("check_type"),
        count("*").alias("issue_count")
    )
    
    print("   • Quality Check:")
    display(quality_check)
    print("   ✓ Validation complete")


# ─── 5. Reconciliation ──────────────────────────────────────────────────────
def reconcile_layers(partition_dt: str):
    """Compare Bronze vs Silver record counts."""
    print("\n5. Reconciling Bronze vs Silver...")
    
    # Bronze stats
    bronze_stats = spark.table(BRONZE_TABLE).filter(col("_partition_dt") == partition_dt).agg(
        lit("BRONZE").alias("layer"),
        count("*").alias("total_records"),
        spark_sum("count").alias("total_flights")
    )
    
    # Silver stats
    silver_stats = spark.table(SILVER_TABLE).filter(
        (col("ingestion_year") == year(current_date())) &
        (col("ingestion_month") == month(current_date()))
    ).agg(
        lit("SILVER").alias("layer"),
        count("*").alias("total_records"),
        spark_sum("flight_count").alias("total_flights")
    )
    
    reconciliation = bronze_stats.union(silver_stats)
    print("   • Layer Comparison:")
    display(reconciliation)
    print("   ✓ Reconciliation complete")


# ─── Entrypoint ─────────────────────────────────────────────────────────────
def run(partition_dt: str = None):
    """Execute Silver layer transformation."""
    partition_dt = partition_dt or str(date.today())
    
    print(f"\n{'='*70}")
    print(f"SILVER LAYER TRANSFORMATION | Partition: {partition_dt}")
    print(f"{'='*70}")
    
    try:
        # Step 1: Create Silver table
        create_silver_table()
        
        # Step 2: Transform Bronze to Silver
        source_df = transform_bronze_to_silver(partition_dt)
        
        # Step 3: Merge into Silver
        merge_to_silver(source_df)
        
        # Step 4: Validate
        validate_silver()
        
        # Step 5: Reconcile
        reconcile_layers(partition_dt)
        
        print(f"\n{'='*70}")
        print("✅ SILVER LAYER TRANSFORMATION COMPLETE")
        print(f"{'='*70}\n")
        
    except Exception as e:
        print(f"\n❌ Silver transformation failed: {str(e)}")
        raise
