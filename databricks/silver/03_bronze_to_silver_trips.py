# Databricks notebook source
# Bronze source path
bronze_path = "abfss://lakehouse@chicagoridedl.dfs.core.windows.net/bronze/trips/year=2025/"

# Read the complete 2025 Bronze dataset
bronze_df = spark.read.parquet(bronze_path)

print(f"Number of columns: {len(bronze_df.columns)}")

bronze_df.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

typed_df = bronze_df.select(
    F.col("trip_id"),

    F.to_timestamp("trip_start_timestamp").alias("trip_start_timestamp"),
    F.to_timestamp("trip_end_timestamp").alias("trip_end_timestamp"),

    F.expr("try_cast(trip_seconds as bigint)").alias("trip_seconds"),
    F.expr("try_cast(trip_miles as double)").alias("trip_miles"),

    F.col("pickup_census_tract"),
    F.col("dropoff_census_tract"),

    F.expr("try_cast(pickup_community_area as int)").alias("pickup_community_area"),
    F.expr("try_cast(dropoff_community_area as int)").alias("dropoff_community_area"),

    F.expr("try_cast(fare as decimal(12,2))").alias("fare"),
    F.expr("try_cast(tip as decimal(12,2))").alias("tip"),
    F.expr("try_cast(additional_charges as decimal(12,2))").alias("additional_charges"),
    F.expr("try_cast(trip_total as decimal(12,2))").alias("trip_total"),

    F.col("shared_trip_authorized"),
    F.col("shared_trip_match"),

    F.expr("try_cast(trips_pooled as int)").alias("trips_pooled"),

    F.expr("try_cast(pickup_centroid_latitude as double)").alias("pickup_centroid_latitude"),
    F.expr("try_cast(pickup_centroid_longitude as double)").alias("pickup_centroid_longitude"),
    F.expr("try_cast(dropoff_centroid_latitude as double)").alias("dropoff_centroid_latitude"),
    F.expr("try_cast(dropoff_centroid_longitude as double)").alias("dropoff_centroid_longitude"),

    F.col("month")
)

typed_df.printSchema()

# COMMAND ----------

# Validate whether datatype conversion introduced any new NULL values

cast_validation = bronze_df.agg(

    F.sum(
        F.when(
            F.col("trip_start_timestamp").isNotNull() &
            F.expr("try_cast(trip_start_timestamp as timestamp)").isNull(),
            1
        ).otherwise(0)
    ).alias("invalid_trip_start_timestamp"),

    F.sum(
        F.when(
            F.col("trip_end_timestamp").isNotNull() &
            F.expr("try_cast(trip_end_timestamp as timestamp)").isNull(),
            1
        ).otherwise(0)
    ).alias("invalid_trip_end_timestamp"),

    F.sum(
        F.when(
            F.col("trip_seconds").isNotNull() &
            F.expr("try_cast(trip_seconds as bigint)").isNull(),
            1
        ).otherwise(0)
    ).alias("invalid_trip_seconds"),

    F.sum(
        F.when(
            F.col("trip_miles").isNotNull() &
            F.expr("try_cast(trip_miles as double)").isNull(),
            1
        ).otherwise(0)
    ).alias("invalid_trip_miles"),

    F.sum(
        F.when(
            F.col("pickup_community_area").isNotNull() &
            F.expr("try_cast(pickup_community_area as int)").isNull(),
            1
        ).otherwise(0)
    ).alias("invalid_pickup_community_area"),

    F.sum(
        F.when(
            F.col("dropoff_community_area").isNotNull() &
            F.expr("try_cast(dropoff_community_area as int)").isNull(),
            1
        ).otherwise(0)
    ).alias("invalid_dropoff_community_area"),

    F.sum(
        F.when(
            F.col("fare").isNotNull() &
            F.expr("try_cast(fare as decimal(12,2))").isNull(),
            1
        ).otherwise(0)
    ).alias("invalid_fare"),

    F.sum(
        F.when(
            F.col("tip").isNotNull() &
            F.expr("try_cast(tip as decimal(12,2))").isNull(),
            1
        ).otherwise(0)
    ).alias("invalid_tip"),

    F.sum(
        F.when(
            F.col("additional_charges").isNotNull() &
            F.expr("try_cast(additional_charges as decimal(12,2))").isNull(),
            1
        ).otherwise(0)
    ).alias("invalid_additional_charges"),

    F.sum(
        F.when(
            F.col("trip_total").isNotNull() &
            F.expr("try_cast(trip_total as decimal(12,2))").isNull(),
            1
        ).otherwise(0)
    ).alias("invalid_trip_total"),

    F.sum(
        F.when(
            F.col("trips_pooled").isNotNull() &
            F.expr("try_cast(trips_pooled as int)").isNull(),
            1
        ).otherwise(0)
    ).alias("invalid_trips_pooled"),

    F.sum(
        F.when(
            F.col("pickup_centroid_latitude").isNotNull() &
            F.expr("try_cast(pickup_centroid_latitude as double)").isNull(),
            1
        ).otherwise(0)
    ).alias("invalid_pickup_latitude"),

    F.sum(
        F.when(
            F.col("pickup_centroid_longitude").isNotNull() &
            F.expr("try_cast(pickup_centroid_longitude as double)").isNull(),
            1
        ).otherwise(0)
    ).alias("invalid_pickup_longitude"),

    F.sum(
        F.when(
            F.col("dropoff_centroid_latitude").isNotNull() &
            F.expr("try_cast(dropoff_centroid_latitude as double)").isNull(),
            1
        ).otherwise(0)
    ).alias("invalid_dropoff_latitude"),

    F.sum(
        F.when(
            F.col("dropoff_centroid_longitude").isNotNull() &
            F.expr("try_cast(dropoff_centroid_longitude as double)").isNull(),
            1
        ).otherwise(0)
    ).alias("invalid_dropoff_longitude")
)

display(cast_validation)

# COMMAND ----------

# Derive duration from timestamps and classify duration quality

duration_df = (
    typed_df

    # Duration calculated independently from start/end timestamps
    .withColumn(
        "derived_trip_seconds",
        F.col("trip_end_timestamp").cast("long")
        - F.col("trip_start_timestamp").cast("long")
    )

    # Classify each record before making any corrections
    .withColumn(
        "duration_quality_status",
        F.when(
            F.col("trip_end_timestamp") < F.col("trip_start_timestamp"),
            F.lit("REVERSED_TIMESTAMP")
        )
        .when(
            (F.col("trip_seconds").isNull() | (F.col("trip_seconds") <= 0))
            & (F.col("derived_trip_seconds") > 0),
            F.lit("RECOVERABLE_DURATION")
        )
        .when(
            (F.col("trip_seconds").isNull() | (F.col("trip_seconds") <= 0))
            & (F.col("derived_trip_seconds") == 0),
            F.lit("ZERO_DURATION")
        )
        .otherwise(
            F.lit("VALID")
        )
    )
)

display(
    duration_df
        .groupBy("duration_quality_status")
        .count()
        .orderBy("duration_quality_status")
)

# COMMAND ----------

# Records that cannot be reliably repaired
quarantine_df = (
    duration_df
    .filter(
        F.col("duration_quality_status").isin(
            "REVERSED_TIMESTAMP",
            "ZERO_DURATION"
        )
    )
)

# Valid Silver records, including recoverable duration records
silver_clean_df = (
    duration_df
    .filter(
        ~F.col("duration_quality_status").isin(
            "REVERSED_TIMESTAMP",
            "ZERO_DURATION"
        )
    )

    # Flag records where trip_seconds is reconstructed
    .withColumn(
        "trip_seconds_imputed",
        F.col("duration_quality_status") == "RECOVERABLE_DURATION"
    )

    # Replace trip_seconds only for the 79 safely recoverable records
    .withColumn(
        "trip_seconds",
        F.when(
            F.col("duration_quality_status") == "RECOVERABLE_DURATION",
            F.col("derived_trip_seconds")
        ).otherwise(F.col("trip_seconds"))
    )
)

# COMMAND ----------

# Validate Silver repair and quarantine logic

validation_df = (
    duration_df
    .agg(
        F.sum(
            F.when(
                F.col("duration_quality_status").isin(
                    "REVERSED_TIMESTAMP",
                    "ZERO_DURATION"
                ),
                1
            ).otherwise(0)
        ).alias("quarantine_rows"),

        F.sum(
            F.when(
                F.col("duration_quality_status") == "RECOVERABLE_DURATION",
                1
            ).otherwise(0)
        ).alias("imputed_rows"),

        F.sum(
            F.when(
                ~F.col("duration_quality_status").isin(
                    "REVERSED_TIMESTAMP",
                    "ZERO_DURATION"
                ),
                1
            ).otherwise(0)
        ).alias("silver_rows")
    )
)

display(validation_df)

# COMMAND ----------

# Add analytical features to the clean Silver dataset

silver_enriched_df = (
    silver_clean_df

    # Duration in minutes
    .withColumn(
        "trip_duration_minutes",
        F.round(F.col("trip_seconds") / 60.0, 2)
    )

    # Fare per mile - only when distance is positive
    .withColumn(
        "fare_per_mile",
        F.when(
            (F.col("trip_miles") > 0) & F.col("fare").isNotNull(),
            F.round(F.col("fare") / F.col("trip_miles"), 2)
        )
    )

    # Average speed in miles per hour
    .withColumn(
        "avg_speed_mph",
        F.when(
            (F.col("trip_seconds") > 0) & (F.col("trip_miles") >= 0),
            F.round(
                F.col("trip_miles") * 3600.0 / F.col("trip_seconds"),
                2
            )
        )
    )

    # Calendar features
    .withColumn(
        "trip_date",
        F.to_date("trip_start_timestamp")
    )
    .withColumn(
        "trip_hour",
        F.hour("trip_start_timestamp")
    )
    .withColumn(
        "trip_day_of_week",
        F.date_format("trip_start_timestamp", "EEEE")
    )
    .withColumn(
        "trip_month",
        F.month("trip_start_timestamp")
    )

    # Weekend flag
    .withColumn(
        "is_weekend",
        F.dayofweek("trip_start_timestamp").isin(1, 7)
    )

    # Time-of-day classification
    .withColumn(
        "time_period",
        F.when(F.col("trip_hour").between(6, 11), "MORNING")
         .when(F.col("trip_hour").between(12, 16), "AFTERNOON")
         .when(F.col("trip_hour").between(17, 21), "EVENING")
         .otherwise("NIGHT")
    )

    # Project-defined weekday commute peak indicator
    .withColumn(
        "is_peak_hour",
        (~F.col("is_weekend")) &
        (
            F.col("trip_hour").between(7, 9) |
            F.col("trip_hour").between(16, 19)
        )
    )

    # Distance bands
    .withColumn(
        "distance_category",
        F.when(F.col("trip_miles") == 0, "ZERO_DISTANCE")
         .when(F.col("trip_miles") <= 2, "SHORT")
         .when(F.col("trip_miles") <= 5, "MEDIUM")
         .when(F.col("trip_miles") <= 10, "LONG")
         .otherwise("VERY_LONG")
    )
)

# COMMAND ----------

silver_enriched_df.select(
    "trip_id",
    "trip_start_timestamp",
    "trip_seconds",
    "trip_seconds_imputed",
    "trip_duration_minutes",
    "trip_miles",
    "fare",
    "fare_per_mile",
    "avg_speed_mph",
    "trip_date",
    "trip_hour",
    "trip_day_of_week",
    "is_weekend",
    "time_period",
    "is_peak_hour",
    "distance_category"
).show(10, truncate=False)

# COMMAND ----------

community_lookup_path = (
    "abfss://lakehouse@chicagoridedl.dfs.core.windows.net/"
    "bronze/pickup_community_area/Community_Areas_id.csv"
)

community_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(community_lookup_path)
)

community_df.printSchema()

display(community_df.limit(10))
print("Lookup row count:", community_df.count())
print("Lookup columns:", community_df.columns)

# COMMAND ----------

# Keep only the columns needed for enrichment

community_lookup_df = (
    community_df
    .select(
        F.col("AREA_NUMBE").cast("int").alias("community_area_id"),
        F.trim(F.col("COMMUNITY")).alias("community_area_name")
    )
)

# Validate uniqueness of the lookup key
display(
    community_lookup_df
    .groupBy("community_area_id")
    .count()
    .filter(F.col("count") > 1)
)

# COMMAND ----------

# Create pickup lookup
pickup_lookup_df = (
    community_lookup_df
    .select(
        F.col("community_area_id").alias("pickup_area_id"),
        F.col("community_area_name").alias("pickup_community_name")
    )
)

# Create dropoff lookup
dropoff_lookup_df = (
    community_lookup_df
    .select(
        F.col("community_area_id").alias("dropoff_area_id"),
        F.col("community_area_name").alias("dropoff_community_name")
    )
)

# Enrich Silver trips using broadcast joins
silver_geo_df = (
    silver_enriched_df

    .join(
        F.broadcast(pickup_lookup_df),
        F.col("pickup_community_area") == F.col("pickup_area_id"),
        "left"
    )
    .drop("pickup_area_id")

    .join(
        F.broadcast(dropoff_lookup_df),
        F.col("dropoff_community_area") == F.col("dropoff_area_id"),
        "left"
    )
    .drop("dropoff_area_id")
)

# COMMAND ----------

geo_validation_df = silver_geo_df.agg(

    F.sum(
        F.when(F.col("pickup_community_area").isNull(), 1).otherwise(0)
    ).alias("missing_pickup_area_id"),

    F.sum(
        F.when(
            F.col("pickup_community_area").isNotNull() &
            F.col("pickup_community_name").isNull(),
            1
        ).otherwise(0)
    ).alias("unmatched_pickup_area_id"),

    F.sum(
        F.when(F.col("dropoff_community_area").isNull(), 1).otherwise(0)
    ).alias("missing_dropoff_area_id"),

    F.sum(
        F.when(
            F.col("dropoff_community_area").isNotNull() &
            F.col("dropoff_community_name").isNull(),
            1
        ).otherwise(0)
    ).alias("unmatched_dropoff_area_id")
)

display(geo_validation_df)

# COMMAND ----------

# Add explicit data-quality flags and analytics-friendly geography labels

silver_final_df = (
    silver_geo_df

    # Keep missing geography visible rather than dropping the trip
    .withColumn(
        "pickup_community_name",
        F.coalesce(
            F.col("pickup_community_name"),
            F.lit("UNKNOWN")
        )
    )
    .withColumn(
        "dropoff_community_name",
        F.coalesce(
            F.col("dropoff_community_name"),
            F.lit("UNKNOWN")
        )
    )

    # Geography quality flags
    .withColumn(
        "pickup_geography_missing",
        F.col("pickup_community_area").isNull()
    )
    .withColumn(
        "dropoff_geography_missing",
        F.col("dropoff_community_area").isNull()
    )

    # Financial quality flag
    .withColumn(
        "financial_data_missing",
        F.col("fare").isNull()
        | F.col("tip").isNull()
        | F.col("additional_charges").isNull()
        | F.col("trip_total").isNull()
    )
)

# COMMAND ----------

display(
    silver_final_df
    .groupBy("financial_data_missing")
    .count()
    .orderBy("financial_data_missing")
)

# COMMAND ----------

final_silver_validation = silver_final_df.agg(

    F.count("*").alias("total_silver_rows"),

    F.sum(
        F.when(F.col("trip_id").isNull(), 1).otherwise(0)
    ).alias("null_trip_id"),

    F.sum(
        F.when(F.col("trip_seconds_imputed") == True, 1).otherwise(0)
    ).alias("imputed_duration_rows"),

    F.sum(
        F.when(
            F.col("trip_seconds").isNull() |
            (F.col("trip_seconds") <= 0),
            1
        ).otherwise(0)
    ).alias("invalid_trip_seconds"),

    F.sum(
        F.when(
            F.col("trip_end_timestamp") < F.col("trip_start_timestamp"),
            1
        ).otherwise(0)
    ).alias("reversed_timestamp_rows"),

    F.sum(
        F.when(
            F.col("pickup_community_name") == "UNKNOWN",
            1
        ).otherwise(0)
    ).alias("unknown_pickup_community"),

    F.sum(
        F.when(
            F.col("dropoff_community_name") == "UNKNOWN",
            1
        ).otherwise(0)
    ).alias("unknown_dropoff_community"),

    F.sum(
        F.when(
            F.col("financial_data_missing") == True,
            1
        ).otherwise(0)
    ).alias("financial_data_missing_rows")
)

display(final_silver_validation)

# COMMAND ----------

silver_path = (
    "abfss://lakehouse@chicagoridedl.dfs.core.windows.net/"
    "silver/trips/"
)

quarantine_path = (
    "abfss://lakehouse@chicagoridedl.dfs.core.windows.net/"
    "silver/quarantine/trips/"
)

# COMMAND ----------

quarantine_final_df = (
    quarantine_df
    .withColumn(
        "quarantine_reason",
        F.when(
            F.col("duration_quality_status") == "REVERSED_TIMESTAMP",
            F.lit("TRIP_END_BEFORE_TRIP_START")
        )
        .when(
            F.col("duration_quality_status") == "ZERO_DURATION",
            F.lit("ZERO_OR_UNRECOVERABLE_DURATION")
        )
    )
)

# COMMAND ----------

(
    silver_final_df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("trip_month")
    .save(silver_path)
)

# COMMAND ----------

(
    quarantine_final_df.write
    .format("delta")
    .mode("overwrite")
    .save(quarantine_path)
)


# COMMAND ----------

silver_written_df = spark.read.format("delta").load(silver_path)
quarantine_written_df = spark.read.format("delta").load(quarantine_path)

print("Silver rows:", silver_written_df.count())
print("Quarantine rows:", quarantine_written_df.count())

# COMMAND ----------

# Validate persisted Silver month coverage

display(
    silver_written_df
    .groupBy("trip_month")
    .count()
    .orderBy("trip_month")
)

# COMMAND ----------

# Validate quarantine reason distribution

display(
    quarantine_written_df
    .groupBy("quarantine_reason")
    .count()
    .orderBy("quarantine_reason")
)

# COMMAND ----------

display(
    spark.sql(
        f"DESCRIBE DETAIL delta.`{silver_path}`"
    )
)