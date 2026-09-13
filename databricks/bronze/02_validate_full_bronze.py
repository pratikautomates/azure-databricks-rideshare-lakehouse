# Databricks notebook source
bronze_path = (
    "abfss://lakehouse@chicagoridedl.dfs.core.windows.net/"
    "bronze/trips/year=2025/"
)

df_bronze = spark.read.parquet(bronze_path)

print("Total Bronze rows:", df_bronze.count())
print("Total columns:", len(df_bronze.columns))

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, month

df_validation = (
    df_bronze
    .withColumn(
        "trip_start_ts",
        to_timestamp(col("trip_start_timestamp"))
    )
)

monthly_counts = (
    df_validation
    .withColumn("month_number", month(col("trip_start_ts")))
    .groupBy("month_number")
    .count()
    .orderBy("month_number")
)

display(monthly_counts)

# COMMAND ----------

from pyspark.sql.functions import min, max

df_validation.select(
    min("trip_start_ts").alias("min_timestamp"),
    max("trip_start_ts").alias("max_timestamp")
).show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import to_date, countDistinct

date_check = (
    df_validation
    .withColumn("trip_date", to_date(col("trip_start_ts")))
)

date_check.select(
    countDistinct("trip_date").alias("distinct_trip_dates")
).show()

# COMMAND ----------

date_counts = (
    date_check
    .groupBy("trip_date")
    .count()
    .orderBy("trip_date")
)

display(date_counts)

# COMMAND ----------

jan_rows = (
    df_validation
    .filter(month(col("trip_start_ts")) == 1)
    .count()
)

print("January rows:", jan_rows)

# COMMAND ----------

from pyspark.sql.functions import when, count

quality_summary = df_bronze.select(
    count("*").alias("total_rows"),

    count(
        when(col("trip_id").isNull(), 1)
    ).alias("null_trip_id"),

    count(
        when(col("trip_start_timestamp").isNull(), 1)
    ).alias("null_start_timestamp"),

    count(
        when(col("trip_end_timestamp").isNull(), 1)
    ).alias("null_end_timestamp"),

    count(
        when(col("trip_seconds").isNull(), 1)
    ).alias("null_trip_seconds"),

    count(
        when(col("trip_miles").isNull(), 1)
    ).alias("null_trip_miles")
)

display(quality_summary)

# COMMAND ----------

duplicate_trip_ids = (
    df_bronze
    .groupBy("trip_id")
    .count()
    .filter(col("count") > 1)
)

print("Duplicate trip IDs:", duplicate_trip_ids.count())

# COMMAND ----------

from pyspark.sql.functions import col, when, count

business_nulls = df_bronze.select(
    count("*").alias("total_rows"),

    count(when(col("fare").isNull(), 1))
        .alias("null_fare"),

    count(when(col("tip").isNull(), 1))
        .alias("null_tip"),

    count(when(col("additional_charges").isNull(), 1))
        .alias("null_additional_charges"),

    count(when(col("trip_total").isNull(), 1))
        .alias("null_trip_total"),

    count(when(col("pickup_community_area").isNull(), 1))
        .alias("null_pickup_community_area"),

    count(when(col("dropoff_community_area").isNull(), 1))
        .alias("null_dropoff_community_area"),

    count(when(col("shared_trip_authorized").isNull(), 1))
        .alias("null_shared_trip_authorized"),

    count(when(col("trips_pooled").isNull(), 1))
        .alias("null_trips_pooled")
)

display(business_nulls)

# COMMAND ----------

# DBTITLE 1,Cell 10
from pyspark.sql.functions import col, to_timestamp, when, count

invalid_summary = df_bronze.select(
    count(when(col("trip_seconds").cast("double") <= 0, 1))
        .alias("invalid_trip_seconds"),

    count(when(col("trip_miles").cast("double") < 0, 1))
        .alias("negative_trip_miles"),

    count(when(col("fare").cast("double") < 0, 1))
        .alias("negative_fare"),

    count(when(col("trip_total").cast("double") < 0, 1))
        .alias("negative_trip_total"),

    count(
        when(
            to_timestamp(col("trip_end_timestamp")) <
            to_timestamp(col("trip_start_timestamp")),
            1
        )
    ).alias("end_before_start")
)

display(invalid_summary)

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

non_positive_duration = (
    col("trip_seconds").cast("double") <= 0
)

bad_timestamp_order = (
    to_timestamp(col("trip_end_timestamp")) <
    to_timestamp(col("trip_start_timestamp"))
)

overlap_count = (
    df_bronze
    .filter(non_positive_duration & bad_timestamp_order)
    .count()
)

print("Overlap count:", overlap_count)

# COMMAND ----------

non_positive_count = (
    df_bronze
    .filter(non_positive_duration)
    .count()
)

bad_timestamp_count = (
    df_bronze
    .filter(bad_timestamp_order)
    .count()
)

only_non_positive = (
    df_bronze
    .filter(non_positive_duration & ~bad_timestamp_order)
    .count()
)

only_bad_timestamp = (
    df_bronze
    .filter(bad_timestamp_order & ~non_positive_duration)
    .count()
)

print("Non-positive trip_seconds:", non_positive_count)
print("End before start:", bad_timestamp_count)
print("Overlap:", overlap_count)
print("Only non-positive trip_seconds:", only_non_positive)
print("Only end-before-start:", only_bad_timestamp)

# COMMAND ----------

bad_timestamp_with_null_seconds = (
    df_bronze
    .filter(
        bad_timestamp_order &
        col("trip_seconds").isNull()
    )
    .count()
)

print(
    "End-before-start rows with null trip_seconds:",
    bad_timestamp_with_null_seconds
)

# COMMAND ----------

only_bad_timestamp = (
    df_bronze
    .filter(
        bad_timestamp_order &
        (
            col("trip_seconds").isNull() |
            (col("trip_seconds").cast("double") > 0)
        )
    )
    .count()
)

print("Only end-before-start:", only_bad_timestamp)

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, unix_timestamp

duration_check = (
    df_bronze
    .withColumn(
        "start_ts",
        to_timestamp(col("trip_start_timestamp"))
    )
    .withColumn(
        "end_ts",
        to_timestamp(col("trip_end_timestamp"))
    )
    .withColumn(
        "derived_trip_seconds",
        unix_timestamp("end_ts") - unix_timestamp("start_ts")
    )
)

recoverable_duration_rows = (
    duration_check
    .filter(
        (
            col("trip_seconds").isNull() |
            (col("trip_seconds").cast("double") <= 0)
        )
        &
        (col("end_ts") >= col("start_ts"))
    )
)

recoverable_duration_rows.select(
    "trip_id",
    "trip_start_timestamp",
    "trip_end_timestamp",
    "trip_seconds",
    "derived_trip_seconds"
).show(20, truncate=False)

print(
    "Potentially recoverable duration rows:",
    recoverable_duration_rows.count()
)

print(
    "Recoverable rows with derived duration > 0:",
    recoverable_duration_rows
        .filter(col("derived_trip_seconds") > 0)
        .count()
)

print(
    "Recoverable rows with derived duration = 0:",
    recoverable_duration_rows
        .filter(col("derived_trip_seconds") == 0)
        .count()
)

# COMMAND ----------

from pyspark.sql.functions import when, count, col

duration_breakdown = (
    recoverable_duration_rows
    .select(
        count(
            when(
                col("trip_seconds").isNull() &
                (col("derived_trip_seconds") > 0),
                1
            )
        ).alias("null_seconds_recoverable"),

        count(
            when(
                (col("trip_seconds").cast("double") <= 0) &
                (col("derived_trip_seconds") > 0),
                1
            )
        ).alias("non_positive_seconds_recoverable"),

        count(
            when(
                col("trip_seconds").isNull() &
                (col("derived_trip_seconds") == 0),
                1
            )
        ).alias("null_seconds_zero_duration"),

        count(
            when(
                (col("trip_seconds").cast("double") <= 0) &
                (col("derived_trip_seconds") == 0),
                1
            )
        ).alias("non_positive_seconds_zero_duration")
    )
)

display(duration_breakdown)