# =============================================================================
# GOLD LAYER TRANSFORMATION - COUNTRY FLIGHT ANALYTICS (PySpark)
# Optimized for Analytics & BI Tool Consumption
# Catalog: databricks_dlt | Schema: sample_dlt_table
# =============================================================================

from pyspark.sql.functions import (
    col, sum, count, countDistinct, avg, round as spark_round, 
    current_timestamp, row_number, lit
)
from pyspark.sql.window import Window

print("=== Starting Gold Layer Transformation ===")

# Read silver table
silver_df = spark.table("databricks_dlt.sample_dlt_table.silver_country_flights")

# =============================================================================
# 1. CREATE GOLD FACT TABLE: fct_country_flights
# =============================================================================
print("\n1. Creating fct_country_flights...")

fct_country_flights = silver_df.select(
    col("dest_country"),
    col("origin_country"),
    col("flight_count"),
    col("ingestion_year"),
    col("ingestion_month"),
    current_timestamp().alias("_gold_created_at"),
    col("_source_batch_id")
)

# Write with Delta optimizations and partitioning
fct_country_flights.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("ingestion_year", "ingestion_month") \
    .option("overwriteSchema", "true") \
    .option("delta.autoOptimize.optimizeWrite", "true") \
    .option("delta.autoOptimize.autoCompact", "true") \
    .saveAsTable("databricks_dlt.sample_dlt_table.fct_country_flights")

print(f"   ✓ Created with {fct_country_flights.count()} records")

# =============================================================================
# 2. CREATE DIMENSION TABLE: dim_destination_country
# =============================================================================
print("\n2. Creating dim_destination_country...")

dim_dest = silver_df.groupBy("dest_country").agg(
    sum("flight_count").cast("int").alias("total_inbound_flights"),
    countDistinct("origin_country").cast("int").alias("unique_origin_countries"),
    spark_round(avg("flight_count"), 2).alias("avg_flights_per_origin"),
    current_timestamp().alias("_created_at"),
    current_timestamp().alias("_updated_at")
).orderBy(col("total_inbound_flights").desc())

dim_dest.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.autoOptimize.optimizeWrite", "true") \
    .option("delta.autoOptimize.autoCompact", "true") \
    .saveAsTable("databricks_dlt.sample_dlt_table.dim_destination_country")

print(f"   ✓ Created with {dim_dest.count()} records")

# =============================================================================
# 3. CREATE DIMENSION TABLE: dim_origin_country
# =============================================================================
print("\n3. Creating dim_origin_country...")

dim_origin = silver_df.groupBy("origin_country").agg(
    sum("flight_count").cast("int").alias("total_outbound_flights"),
    countDistinct("dest_country").cast("int").alias("unique_dest_countries"),
    spark_round(avg("flight_count"), 2).alias("avg_flights_per_dest"),
    current_timestamp().alias("_created_at"),
    current_timestamp().alias("_updated_at")
).orderBy(col("total_outbound_flights").desc())

dim_origin.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.autoOptimize.optimizeWrite", "true") \
    .option("delta.autoOptimize.autoCompact", "true") \
    .saveAsTable("databricks_dlt.sample_dlt_table.dim_origin_country")

print(f"   ✓ Created with {dim_origin.count()} records")

# =============================================================================
# 4. CREATE AGGREGATION TABLE: agg_top_routes
# =============================================================================
print("\n4. Creating agg_top_routes...")

window_spec = Window.orderBy(col("flight_count").desc())

agg_routes = silver_df.select(
    col("origin_country"),
    col("dest_country"),
    col("flight_count").alias("total_flights"),
    row_number().over(window_spec).alias("route_rank"),
    current_timestamp().alias("_created_at")
).filter(col("route_rank") <= 100)

agg_routes.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.autoOptimize.optimizeWrite", "true") \
    .option("delta.autoOptimize.autoCompact", "true") \
    .saveAsTable("databricks_dlt.sample_dlt_table.agg_top_routes")

print(f"   ✓ Created with {agg_routes.count()} records")

# =============================================================================
# 5. CREATE COUNTRY PERFORMANCE TABLE
# =============================================================================
print("\n5. Creating agg_country_performance...")

# Inbound (destination) metrics
inbound_df = silver_df.groupBy("dest_country").agg(
    sum("flight_count").cast("int").alias("total_flights"),
    countDistinct("origin_country").cast("int").alias("unique_connections")
).select(
    col("dest_country").alias("country"),
    lit("INBOUND").alias("direction"),
    col("total_flights"),
    col("unique_connections")
)

# Outbound (origin) metrics
outbound_df = silver_df.groupBy("origin_country").agg(
    sum("flight_count").cast("int").alias("total_flights"),
    countDistinct("dest_country").cast("int").alias("unique_connections")
).select(
    col("origin_country").alias("country"),
    lit("OUTBOUND").alias("direction"),
    col("total_flights"),
    col("unique_connections")
)

# Combine and rank
combined_df = inbound_df.union(outbound_df)
window_spec_perf = Window.partitionBy("direction").orderBy(col("total_flights").desc())

agg_performance = combined_df.select(
    col("country"),
    col("direction"),
    col("total_flights"),
    col("unique_connections"),
    row_number().over(window_spec_perf).alias("performance_rank"),
    current_timestamp().alias("_created_at")
).orderBy("direction", "performance_rank")

agg_performance.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.autoOptimize.optimizeWrite", "true") \
    .option("delta.autoOptimize.autoCompact", "true") \
    .saveAsTable("databricks_dlt.sample_dlt_table.agg_country_performance")

print(f"   ✓ Created with {agg_performance.count()} records")

# =============================================================================
# VERIFICATION
# =============================================================================
print("\n=== Verification: Record Counts ===")

verification_data = [
    ("fct_country_flights", spark.table("databricks_dlt.sample_dlt_table.fct_country_flights").count()),
    ("dim_destination_country", spark.table("databricks_dlt.sample_dlt_table.dim_destination_country").count()),
    ("dim_origin_country", spark.table("databricks_dlt.sample_dlt_table.dim_origin_country").count()),
    ("agg_top_routes", spark.table("databricks_dlt.sample_dlt_table.agg_top_routes").count()),
    ("agg_country_performance", spark.table("databricks_dlt.sample_dlt_table.agg_country_performance").count())
]

verification_df = spark.createDataFrame(verification_data, ["table_name", "record_count"])
display(verification_df)

print("\n✅ Gold layer intermediate tables created successfully!")



# =============================================================================
# CONSOLIDATED GOLD TABLE: gold_flight_analytics_consolidated (PySpark)
# =============================================================================

from pyspark.sql.functions import (
    col, round as spark_round, lit, when, current_timestamp,
    count, sum, avg, countDistinct, max as spark_max, min as spark_min
)

print("=== Creating Consolidated Gold Table ===")

# Read all dimension and fact tables
fcf = spark.table("databricks_dlt.sample_dlt_table.fct_country_flights")
doc = spark.table("databricks_dlt.sample_dlt_table.dim_origin_country")
ddc = spark.table("databricks_dlt.sample_dlt_table.dim_destination_country")
atr = spark.table("databricks_dlt.sample_dlt_table.agg_top_routes")
acp = spark.table("databricks_dlt.sample_dlt_table.agg_country_performance")

# Split performance table
acp_out = acp.filter(col("direction") == "OUTBOUND").select(
    col("country").alias("country_out"),
    col("performance_rank").alias("origin_outbound_rank")
)
acp_in = acp.filter(col("direction") == "INBOUND").select(
    col("country").alias("country_in"),
    col("performance_rank").alias("dest_inbound_rank")
)

# Build consolidated table with joins
consolidated_df = fcf \
    .join(doc, fcf.origin_country == doc.origin_country, "left") \
    .join(ddc, fcf.dest_country == ddc.dest_country, "left") \
    .join(atr, (fcf.origin_country == atr.origin_country) & (fcf.dest_country == atr.dest_country), "left") \
    .join(acp_out, fcf.origin_country == acp_out.country_out, "left") \
    .join(acp_in, fcf.dest_country == acp_in.country_in, "left") \
    .select(
        fcf.origin_country, fcf.dest_country,
        fcf.flight_count.alias("route_flight_count"), atr.route_rank,
        doc.total_outbound_flights.alias("origin_total_outbound_flights"),
        doc.unique_dest_countries.alias("origin_unique_destinations"),
        doc.avg_flights_per_dest.alias("origin_avg_flights_per_dest"),
        acp_out.origin_outbound_rank,
        ddc.total_inbound_flights.alias("dest_total_inbound_flights"),
        ddc.unique_origin_countries.alias("dest_unique_origins"),
        ddc.avg_flights_per_origin.alias("dest_avg_flights_per_origin"),
        acp_in.dest_inbound_rank,
        spark_round(lit(100.0) * fcf.flight_count / doc.total_outbound_flights, 2).alias("route_share_of_origin_pct"),
        spark_round(lit(100.0) * fcf.flight_count / ddc.total_inbound_flights, 2).alias("route_share_of_dest_pct"),
        spark_round((fcf.flight_count.cast("double")) * 
                    (lit(1.0) / when(atr.route_rank == 0, lit(1)).otherwise(atr.route_rank)) * 
                    (lit(1.0) / when(acp_out.origin_outbound_rank == 0, lit(1)).otherwise(acp_out.origin_outbound_rank)) * 
                    (lit(1.0) / when(acp_in.dest_inbound_rank == 0, lit(1)).otherwise(acp_in.dest_inbound_rank)), 4
                   ).alias("route_importance_score"),
        fcf.ingestion_year, fcf.ingestion_month,
        current_timestamp().alias("_consolidated_at")
    ).orderBy(col("route_flight_count").desc())

# Write consolidated table
consolidated_df.write.format("delta").mode("overwrite") \
    .partitionBy("ingestion_year", "ingestion_month") \
    .option("overwriteSchema", "true") \
    .option("delta.autoOptimize.optimizeWrite", "true") \
    .option("delta.autoOptimize.autoCompact", "true") \
    .option("delta.enableChangeDataFeed", "true") \
    .option("delta.dataSkippingNumIndexedCols", "15") \
    .saveAsTable("databricks_dlt.sample_dlt_table.gold_flight_analytics_consolidated")

print(f"✓ Created with {consolidated_df.count()} records")

# Analytics: Top 10 Routes
print("\n=== Top 10 Routes ===")
display(spark.table("databricks_dlt.sample_dlt_table.gold_flight_analytics_consolidated") \
    .select("origin_country", "dest_country", "route_flight_count", "route_rank", "route_importance_score") \
    .orderBy(col("route_importance_score").desc()).limit(10))

# Summary Statistics
print("\n=== Summary ===")
display(spark.table("databricks_dlt.sample_dlt_table.gold_flight_analytics_consolidated").agg(
    count("*").alias("total_routes"), sum("route_flight_count").alias("total_flights"),
    spark_round(avg("route_flight_count"), 2).alias("avg_flights_per_route"),
    spark_max("route_flight_count").alias("max"), spark_min("route_flight_count").alias("min"),
    countDistinct("origin_country").alias("unique_origins"),
    countDistinct("dest_country").alias("unique_destinations")
))

print("\n✅ Gold table complete!")

# =============================================================================
# CLEANUP: DROP INTERMEDIATE GOLD TABLES (PySpark)
# =============================================================================
# Keep only the main Bronze, Silver, and Gold tables:
# - bronze_orders (source data)
# - silver_country_flights (cleaned/transformed data)
# - gold_flight_analytics_consolidated (final analytics table)

print("=== Cleaning up intermediate tables ===")

tables_to_drop = [
    "databricks_dlt.sample_dlt_table.silver_orders",
    "databricks_dlt.sample_dlt_table.fct_country_flights",
    "databricks_dlt.sample_dlt_table.dim_destination_country",
    "databricks_dlt.sample_dlt_table.dim_origin_country",
    "databricks_dlt.sample_dlt_table.agg_top_routes",
    "databricks_dlt.sample_dlt_table.agg_country_performance"
]

for table in tables_to_drop:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        print(f"   ✓ Dropped {table.split('.')[-1]}")
    except Exception as e:
        print(f"   ⚠️ Could not drop {table.split('.')[-1]}: {str(e)}")

print("\n=== Remaining Tables ===")
remaining_tables = spark.sql("""
    SHOW TABLES IN databricks_dlt.sample_dlt_table
""").select("database", "tableName", "isTemporary")

display(remaining_tables)

print("\n✅ Cleanup complete! Only core Bronze-Silver-Gold tables remain.")
