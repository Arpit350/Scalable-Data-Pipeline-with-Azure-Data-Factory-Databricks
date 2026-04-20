# Databricks notebook source file
# Bronze Layer — Raw Data Ingestion from ADLS Gen2

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze Layer: Raw Data Landing
# MAGIC Reads raw files from ADLS Gen2 and writes to Delta format with minimal transformation.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
import os

# COMMAND ----------

# Configuration — values injected via ADF pipeline parameters or Databricks widgets
dbutils.widgets.text("source_path", "", "Source Path (ADLS raw zone)")
dbutils.widgets.text("target_database", "bronze", "Target Database")
dbutils.widgets.text("target_table", "raw_events", "Target Table")
dbutils.widgets.text("batch_date", "", "Batch Date (YYYY-MM-DD)")

source_path   = dbutils.widgets.get("source_path")
target_db     = dbutils.widgets.get("target_database")
target_table  = dbutils.widgets.get("target_table")
batch_date    = dbutils.widgets.get("batch_date")

print(f"Source: {source_path}")
print(f"Target: {target_db}.{target_table}")
print(f"Batch Date: {batch_date}")

# COMMAND ----------

# Read raw data from ADLS Gen2
df_raw = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("badRecordsPath", f"/mnt/adls/bad_records/{target_table}/{batch_date}")
    .csv(source_path)
)

print(f"Records read: {df_raw.count()}")
df_raw.printSchema()

# COMMAND ----------

# Add pipeline metadata columns
df_bronze = (df_raw
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_file", input_file_name())
    .withColumn("_batch_date", lit(batch_date))
)

# COMMAND ----------

# Create database if not exists
spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_db}")

# Write to Delta table (append mode for batch)
(df_bronze.write
    .format("delta")
    .mode("append")
    .partitionBy("_batch_date")
    .saveAsTable(f"{target_db}.{target_table}")
)

print(f"✅ Successfully written to {target_db}.{target_table}")
