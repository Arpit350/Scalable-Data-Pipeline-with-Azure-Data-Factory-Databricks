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

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import date

import sys
sys.path.append("..")

from config.pipeline_config import (
    CATALOG, SCHEMA, BRONZE_TABLE,
    BRONZE_SOURCE_PATH, SOURCE_FORMAT,
)
from utils.spark_utils import get_spark, get_logger, apply_delta_properties, assert_not_empty

logger = get_logger("bronze_ingestion")

# ─── Expected source schema (type enforcement) ────────────────────────────────
# Define the schema explicitly — avoids schema inference surprises in production
BRONZE_SCHEMA = T.StructType([
    T.StructField("order_id",         T.StringType(),    nullable=False),
    T.StructField("customer_id",      T.StringType(),    nullable=False),
    T.StructField("product_id",       T.StringType(),    nullable=True),
    T.StructField("product_category", T.StringType(),    nullable=True),
    T.StructField("quantity",         T.IntegerType(),   nullable=True),
    T.StructField("unit_price",       T.DoubleType(),    nullable=True),
    T.StructField("order_date",       T.StringType(),    nullable=True),   # raw string — cast in Silver
    T.StructField("status",           T.StringType(),    nullable=True),
    T.StructField("region",           T.StringType(),    nullable=True),
])


# ─── 1. Read raw files ────────────────────────────────────────────────────────
def read_raw(spark: SparkSession) -> DataFrame:
    """
    Read source files from UC Volume path.
    Uses explicit schema to enforce types at ingestion.
    Bad records are quarantined via PERMISSIVE mode + _corrupt_record column.
    """
    logger.info(f"Reading {SOURCE_FORMAT} files from: {BRONZE_SOURCE_PATH}")

    df = (
        spark.read
        .format(SOURCE_FORMAT)
        .schema(BRONZE_SCHEMA)
        .option("mode",            "PERMISSIVE")    # keep malformed rows
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("multiLine",       "true")           # for nested JSON
        .option("dateFormat",      "yyyy-MM-dd")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .load(BRONZE_SOURCE_PATH)
    )

    return df


# ─── 2. Schema validation ──────────────────────────────────────────────────────
def validate_schema(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Split DataFrame into:
      - valid_df   : rows that parsed correctly
      - corrupt_df : rows with parse failures (_corrupt_record is not null)

    Corrupt rows are logged for ops alerting; valid rows proceed to Delta write.
    """
    if "_corrupt_record" not in df.columns:
        return df, None

    corrupt_df = df.filter(F.col("_corrupt_record").isNotNull())
    valid_df   = df.filter(F.col("_corrupt_record").isNull()) \
                   .drop("_corrupt_record")

    corrupt_count = corrupt_df.count()
    if corrupt_count > 0:
        logger.warning(f"Corrupt records detected: {corrupt_count:,} rows quarantined.")
        # Optionally write corrupt rows to a quarantine table
        # corrupt_df.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.quarantine_orders")

    return valid_df, corrupt_df


# ─── 3. Attach ingestion metadata ─────────────────────────────────────────────
def add_audit_columns(df: DataFrame, batch_id: str) -> DataFrame:
    """
    Attach Bronze audit columns:
      _ingested_at  : timestamp of when the record was loaded
      _source_file  : full path of the originating file (lineage)
      _batch_id     : logical batch / run identifier
      _partition_dt : date string used as the partition column
    """
    return (
        df
        .withColumn("_ingested_at",  F.current_timestamp())
        .withColumn("_source_file",  F.input_file_name())
        .withColumn("_batch_id",     F.lit(batch_id))
        .withColumn("_partition_dt", F.lit(str(date.today())))   # yyyy-MM-dd
    )


# ─── 4. Write to Delta (partitioned by ingestion date) ────────────────────────
def write_bronze(df: DataFrame, spark: SparkSession) -> None:
    """
    Append-only write to Bronze Delta table.
    Partitioned by _partition_dt to enable date-based pruning.
    mergeSchema=true handles safe schema evolution.
    """
    logger.info(f"Writing Bronze Delta table: {BRONZE_TABLE}")

    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy("_partition_dt")                    # ← date partition for pruning
        .option("mergeSchema", "true")
        .saveAsTable(BRONZE_TABLE)
    )

    # Apply standard Delta properties (auto-optimize, CDF, etc.)
    apply_delta_properties(spark, BRONZE_TABLE)
    logger.info(f"Bronze write complete → {BRONZE_TABLE}")


# ─── 5. Post-write validation ─────────────────────────────────────────────────
def validate_write(spark: SparkSession) -> None:
    """Quick sanity check — confirm rows landed and partition exists."""
    result = spark.sql(f"""
        SELECT
            _partition_dt,
            COUNT(*)          AS row_count,
            COUNT(DISTINCT order_id) AS unique_orders
        FROM {BRONZE_TABLE}
        WHERE _partition_dt = CAST(CURRENT_DATE AS STRING)
        GROUP BY _partition_dt
    """)
    result.show(truncate=False)


# ─── Entrypoint ───────────────────────────────────────────────────────────────
def run(batch_id: str = None) -> None:
    spark    = get_spark("Bronze_Ingestion")
    batch_id = batch_id or str(date.today())

    logger.info(f"=== BRONZE LAYER START | batch_id={batch_id} ===")

    raw_df              = read_raw(spark)
    valid_df, corrupt_df = validate_schema(raw_df)

    assert_not_empty(valid_df, "BRONZE", logger)

    bronze_df = add_audit_columns(valid_df, batch_id)
    write_bronze(bronze_df, spark)
    validate_write(spark)

    logger.info("=== BRONZE LAYER COMPLETE ===")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_id", default=None, help="Logical batch identifier")
    args = parser.parse_args()
    run(batch_id=args.batch_id)
