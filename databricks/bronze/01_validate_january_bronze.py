# Databricks notebook source
# MAGIC %md
# MAGIC # January 2025 Bronze Validation
# MAGIC
# MAGIC Validates the January 2025 Chicago rideshare historical backfill written to the ADLS Gen2 Bronze layer.
# MAGIC
# MAGIC Validation checks:
# MAGIC - Expected Parquet files: 153
# MAGIC - Expected source rows: 7,607,290
# MAGIC - Expected rows per page: 50,000 for pages 1-152
# MAGIC - Expected final page rows: 7,290

# COMMAND ----------

bronze_jan_path = (
    "abfss://bronze@chicagoridedl.dfs.core.windows.net/"
    "trips/year=2025/month=01/"
)

# COMMAND ----------

files = dbutils.fs.ls(bronze_jan_path)

parquet_files = [
    f for f in files
    if f.name.endswith(".parquet")
]

print("Parquet files:", len(parquet_files))

# COMMAND ----------

df_jan = spark.read.parquet(bronze_jan_path)

# COMMAND ----------

jan_count = df_jan.count()
print("January rows:", jan_count)

# COMMAND ----------

from pyspark.sql.functions import col, count

file_counts = (
    df_jan
    .withColumn("source_file", col("_metadata.file_path"))
    .groupBy("source_file")
    .agg(count("*").alias("row_count"))
    .orderBy("source_file")
)

display(file_counts)

# COMMAND ----------

expected_files = 153
expected_rows = 7607290

actual_files = len(parquet_files)
actual_rows = df_jan.count()

print(f"Expected files : {expected_files}")
print(f"Actual files   : {actual_files}")
print(f"Expected rows  : {expected_rows}")
print(f"Actual rows    : {actual_rows}")

if actual_files == expected_files and actual_rows == expected_rows:
    print("✅ January Bronze validation PASSED")
else:
    print("❌ January Bronze validation FAILED")
