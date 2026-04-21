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

# ──────── Create Bronze Delta Table from UC Volume ────────────────────────────────────────────────────────
%sql
CREATE OR REPLACE TABLE `databricks_dlt.sample_dlt_table.bronze_orders` AS
SELECT
    DEST_COUNTRY_NAME,
    ORIGIN_COUNTRY_NAME,
    count,
    current_timestamp() AS _ingested_at,
    input_file_name() AS _source_file,
    current_date() AS _batch_id,
    cast(current_date() as string) AS _partition_dt
FROM
    read_files(
        '/Volumes/databricks_dlt/sample_dlt_table/data_dlt/*.csv',
        format => 'csv',
        header => true,
        mode => 'PERMISSIVE'
    )

# ───  Bronze Layer with Data Quality & Transformations ────────────────────────────────────────────────────────
%sql
CREATE TABLE IF NOT EXISTS databricks_dlt.sample_dlt_table.bronze_orders (
    DEST_COUNTRY_NAME STRING,
    ORIGIN_COUNTRY_NAME STRING,
    count INT,
    _rescued_data STRING,
    _ingested_at TIMESTAMP,
    _source_file STRING,
    _batch_id STRING,
    _partition_dt STRING
)
USING DELTA
PARTITIONED BY (_partition_dt)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true'
);

# ─── Insert with transformations  ────────────────────────────────────────────────────────
INSERT INTO databricks_dlt.sample_dlt_table.bronze_orders
SELECT
    DEST_COUNTRY_NAME,
    ORIGIN_COUNTRY_NAME,
    count,
    _rescued_data,
    current_timestamp() AS _ingested_at,
    input_file_name() AS _source_file,
    cast(current_date() as string) AS _batch_id,
    cast(current_date() as string) AS _partition_dt
FROM
    read_files(
        '/Volumes/databricks_dlt/sample_dlt_table/data_dlt/*.csv',
        format => 'csv',
        header => true,
        mode => 'PERMISSIVE'
    )


# ─── Quarantine Bad Records ──────────────────────────────────────────────────────
%sql
CREATE TABLE IF NOT EXISTS databricks_dlt.sample_dlt_table.quarantine_orders (
    raw_content STRING,
    error_reason STRING,
    _ingested_at TIMESTAMP,
    _batch_id STRING
)
USING DELTA;

INSERT INTO databricks_dlt.sample_dlt_table.quarantine_orders
SELECT
    concat_ws('|', DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, cast(count as string)) AS raw_content,
    CASE 
        WHEN DEST_COUNTRY_NAME IS NULL OR ORIGIN_COUNTRY_NAME IS NULL THEN 'Missing required fields'
        WHEN count IS NULL OR count < 0 THEN 'Invalid count value'
        ELSE 'Other error'
    END AS error_reason,
    current_timestamp() AS _ingested_at,
    cast(current_date() as string) AS _batch_id
FROM
    read_files(
        '/Volumes/databricks_dlt/sample_dlt_table/data_dlt/*.csv',
        format => 'csv',
        header => true
    )
WHERE
    DEST_COUNTRY_NAME IS NULL 
    OR ORIGIN_COUNTRY_NAME IS NULL
    OR count IS NULL
    OR count < 0

# ─── Validation & Monitoring ─────────────────────────────────────────────
%sql
-- Ingestion stats
SELECT
    _partition_dt,
    COUNT(*) AS total_rows
FROM databricks_dlt.sample_dlt_table.bronze_orders
GROUP BY _partition_dt
ORDER BY _partition_dt DESC;

-- Data quality metrics
SELECT
    COUNT(*) AS total_records,
    COUNT(CASE WHEN count IS NULL THEN 1 END) AS null_count,
    COUNT(CASE WHEN DEST_COUNTRY_NAME IS NULL THEN 1 END) AS null_dest_country,
    COUNT(CASE WHEN ORIGIN_COUNTRY_NAME IS NULL THEN 1 END) AS null_origin_country,
    COUNT(DISTINCT _batch_id) AS batches_processed
FROM databricks_dlt.sample_dlt_table.bronze_orders
WHERE _partition_dt = CAST(current_date() AS STRING);

