# =============================================================================
# bronze/bronze_ingestion.py
# Bronze Layer — Raw Ingestion
#
# Responsibilities:
#   1. Read raw files from ADLS Gen2 via Unity Catalog Volume
#   2. Apply schema validation and type enforcement
#   3. Attach ingestion metadata (_ingested_at, _source_file, _batch_id)
#   4. Partition by ingestion date for performance
#   5. Write as Delta table registered in Unity Catalog
# =============================================================================


# ───   Create Bronze Orders Table────────────────────────────────────────────────────────

from pyspark.sql.functions import current_timestamp, current_date, input_file_name, col

# Read CSV files from Volume
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("mode", "PERMISSIVE") \
    .load("/Volumes/databricks_dlt/sample_dlt_table/data_dlt/*.csv")

# Add metadata columns
bronze_df = df.select(
    col("DEST_COUNTRY_NAME"),
    col("ORIGIN_COUNTRY_NAME"),
    col("count").cast("int"),
    current_timestamp().alias("_ingested_at"),
    col("_metadata.file_path").alias("_source_file"),
    current_date().alias("_batch_id"),
    current_date().cast("string").alias("_partition_dt")
)

# Write to Delta table (overwrite mode = CREATE OR REPLACE)
bronze_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("databricks_dlt.sample_dlt_table.bronze_orders")

print(f"Bronze table created with {bronze_df.count()} records")

# ─── Create Quarantine Bad Records Table ──────────────────────────────────────────────────────
from pyspark.sql.functions import concat_ws, when, lit, current_timestamp, current_date, col

# Read CSV files
df = spark.read.format("csv") \
    .option("header", "true") \
    .load("/Volumes/databricks_dlt/sample_dlt_table/data_dlt/*.csv")

# Filter for bad records
bad_records = df.filter(
    col("DEST_COUNTRY_NAME").isNull() | 
    col("ORIGIN_COUNTRY_NAME").isNull() | 
    col("count").isNull() | 
    (col("count").cast("int") < 0)
)

# Create quarantine DataFrame with error reasons
quarantine_df = bad_records.select(
    concat_ws("|", 
              col("DEST_COUNTRY_NAME"), 
              col("ORIGIN_COUNTRY_NAME"), 
              col("count").cast("string")
             ).alias("raw_content"),
    when((col("DEST_COUNTRY_NAME").isNull()) | (col("ORIGIN_COUNTRY_NAME").isNull()), 
         lit("Missing required fields"))
    .when((col("count").isNull()) | (col("count").cast("int") < 0), 
          lit("Invalid count value"))
    .otherwise(lit("Other error"))
    .alias("error_reason"),
    current_timestamp().alias("_ingested_at"),
    current_date().cast("string").alias("_batch_id")
)

# Write to quarantine table (append mode)
quarantine_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("databricks_dlt.sample_dlt_table.quarantine_orders")

print(f"Quarantined {quarantine_df.count()} bad records")

# ─── Data Quality Metrics  ──────────────────────────────────────────────────────
# Ingestion stats by partition
print("=== Ingestion Stats ===")
ingestion_stats = spark.sql("""
    SELECT
        _partition_dt,
        COUNT(*) AS total_rows
    FROM databricks_dlt.sample_dlt_table.bronze_orders
    GROUP BY _partition_dt
    ORDER BY _partition_dt DESC
""")

display(ingestion_stats)

# Data quality metrics for today's partition
print("\n=== Data Quality Metrics (Today) ===")
quality_metrics = spark.sql("""
    SELECT
        COUNT(*) AS total_records,
        COUNT(CASE WHEN count IS NULL THEN 1 END) AS null_count,
        COUNT(CASE WHEN DEST_COUNTRY_NAME IS NULL THEN 1 END) AS null_dest_country,
        COUNT(CASE WHEN ORIGIN_COUNTRY_NAME IS NULL THEN 1 END) AS null_origin_country,
        COUNT(DISTINCT _batch_id) AS batches_processed
    FROM databricks_dlt.sample_dlt_table.bronze_orders
    WHERE _partition_dt = CAST(current_date() AS STRING)
""")

display(quality_metrics)s
WHERE _partition_dt = CAST(current_date() AS STRING);

