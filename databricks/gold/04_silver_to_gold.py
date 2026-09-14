# Databricks notebook source
from pyspark.sql import functions as F

silver_path = (
    "abfss://lakehouse@chicagoridedl.dfs.core.windows.net/"
    "silver/trips/"
)

silver_df = (
    spark.read
    .format("delta")
    .load(silver_path)
)

print("Silver columns:", len(silver_df.columns))
silver_df.printSchema()

# COMMAND ----------

# Gold table 1: Daily business metrics

daily_metrics_df = (
    silver_df
    .groupBy(
        "trip_date",
        "trip_day_of_week",
        "is_weekend"
    )
    .agg(
        # Demand
        F.count("*").alias("total_trips"),

        # Financial coverage
        F.sum(
            F.when(~F.col("financial_data_missing"), 1).otherwise(0)
        ).alias("trips_with_financial_data"),

        # Financial metrics
        F.round(F.sum("fare"), 2).alias("total_fare_amount"),
        F.round(F.sum("tip"), 2).alias("total_tip_amount"),
        F.round(F.sum("trip_total"), 2).alias("total_trip_value"),
        F.round(F.avg("fare"), 2).alias("avg_fare"),
        F.round(F.avg("trip_total"), 2).alias("avg_trip_value"),

        # Trip metrics
        F.round(F.avg("trip_miles"), 2).alias("avg_trip_miles"),
        F.round(F.avg("trip_duration_minutes"), 2).alias(
            "avg_trip_duration_minutes"
        ),
        F.round(F.avg("avg_speed_mph"), 2).alias("avg_speed_mph"),

        # Behaviour / demand segments
        F.sum(
            F.when(F.col("shared_trip_match") == True, 1).otherwise(0)
        ).alias("shared_trip_count"),

        F.sum(
            F.when(F.col("is_peak_hour") == True, 1).otherwise(0)
        ).alias("peak_hour_trip_count")
    )

    # Useful calendar fields for BI
    .withColumn(
        "year",
        F.year("trip_date")
    )
    .withColumn(
        "month",
        F.month("trip_date")
    )
    .withColumn(
        "day_of_month",
        F.dayofmonth("trip_date")
    )
    .orderBy("trip_date")
)

# COMMAND ----------

display(daily_metrics_df.limit(10))

# COMMAND ----------

daily_validation_df = daily_metrics_df.agg(
    F.count("*").alias("number_of_days"),
    F.min("trip_date").alias("min_date"),
    F.max("trip_date").alias("max_date"),
    F.sum("total_trips").alias("reconciled_silver_trips")
)

display(daily_validation_df)

# COMMAND ----------

# Gold table 2: Hourly demand metrics

hourly_demand_df = (
    silver_df
    .groupBy(
        "trip_date",
        "trip_hour",
        "trip_day_of_week",
        "is_weekend",
        "time_period",
        "is_peak_hour"
    )
    .agg(
        F.count("*").alias("total_trips"),

        F.round(
            F.avg("trip_duration_minutes"),
            2
        ).alias("avg_trip_duration_minutes"),

        F.round(
            F.avg("trip_miles"),
            2
        ).alias("avg_trip_miles"),

        F.round(
            F.avg("fare"),
            2
        ).alias("avg_fare"),

        F.round(
            F.sum("trip_total"),
            2
        ).alias("total_trip_value"),

        F.sum(
            F.when(
                F.col("shared_trip_match") == True,
                1
            ).otherwise(0)
        ).alias("shared_trip_count")
    )
    .withColumn(
        "year",
        F.year("trip_date")
    )
    .withColumn(
        "month",
        F.month("trip_date")
    )
)

# COMMAND ----------

hourly_validation_df = hourly_demand_df.agg(
    F.count("*").alias("hourly_rows"),
    F.min("trip_date").alias("min_date"),
    F.max("trip_date").alias("max_date"),
    F.sum("total_trips").alias("reconciled_silver_trips")
)

display(hourly_validation_df)

# COMMAND ----------

# Identify any date-hour combinations with no trips

calendar_hours_df = (
    spark.range(0, 365)
    .select(
        F.date_add(
            F.lit("2025-01-01").cast("date"),
            F.col("id").cast("int")
        ).alias("trip_date")
    )
    .crossJoin(
        spark.range(0, 24)
        .select(F.col("id").cast("int").alias("trip_hour"))
    )
)

observed_hours_df = (
    hourly_demand_df
    .select("trip_date", "trip_hour")
    .distinct()
)

missing_hours_df = (
    calendar_hours_df
    .join(
        observed_hours_df,
        ["trip_date", "trip_hour"],
        "left_anti"
    )
)

display(missing_hours_df)

# COMMAND ----------

# Prepare pickup-side community metrics

pickup_area_df = (
    silver_df
    .withColumn(
        "community_area_id",
        F.coalesce(
            F.col("pickup_community_area"),
            F.lit(0)
        )
    )
    .withColumn(
        "community_area_name",
        F.col("pickup_community_name")
    )
    .groupBy(
        "trip_date",
        "community_area_id",
        "community_area_name"
    )
    .agg(
        F.count("*").alias("pickup_trips"),

        F.round(
            F.sum("trip_total"),
            2
        ).alias("pickup_trip_value"),

        F.round(
            F.avg("fare"),
            2
        ).alias("avg_pickup_fare"),

        F.round(
            F.avg("trip_miles"),
            2
        ).alias("avg_pickup_trip_miles"),

        F.round(
            F.avg("trip_duration_minutes"),
            2
        ).alias("avg_pickup_trip_duration_minutes"),

        F.sum(
            F.when(
                F.col("is_peak_hour") == True,
                1
            ).otherwise(0)
        ).alias("pickup_peak_hour_trips"),

        F.sum(
            F.when(
                F.col("shared_trip_match") == True,
                1
            ).otherwise(0)
        ).alias("pickup_shared_trips")
    )
)

# COMMAND ----------

# Prepare dropoff-side community metrics

dropoff_area_df = (
    silver_df
    .withColumn(
        "community_area_id",
        F.coalesce(
            F.col("dropoff_community_area"),
            F.lit(0)
        )
    )
    .withColumn(
        "community_area_name",
        F.col("dropoff_community_name")
    )
    .groupBy(
        "trip_date",
        "community_area_id",
        "community_area_name"
    )
    .agg(
        F.count("*").alias("dropoff_trips")
    )
)

# COMMAND ----------

community_area_metrics_df = (
    pickup_area_df
    .join(
        dropoff_area_df,
        [
            "trip_date",
            "community_area_id",
            "community_area_name"
        ],
        "full"
    )

    .fillna(
        {
            "pickup_trips": 0,
            "dropoff_trips": 0,
            "pickup_peak_hour_trips": 0,
            "pickup_shared_trips": 0
        }
    )

    # Difference between trips originating and ending in an area
    .withColumn(
        "net_trip_flow",
        F.col("pickup_trips") - F.col("dropoff_trips")
    )

    .withColumn(
        "year",
        F.year("trip_date")
    )

    .withColumn(
        "month",
        F.month("trip_date")
    )
)

# COMMAND ----------

community_validation_df = community_area_metrics_df.agg(

    F.count("*").alias("community_metric_rows"),

    F.sum("pickup_trips").alias("reconciled_pickup_trips"),

    F.sum("dropoff_trips").alias("reconciled_dropoff_trips"),

    F.min("trip_date").alias("min_date"),

    F.max("trip_date").alias("max_date")
)

display(community_validation_df)

# COMMAND ----------

# Gold table 4: Monthly route-level metrics

route_metrics_df = (
    silver_df

    # Preserve missing geography as an explicit UNKNOWN route
    .withColumn(
        "pickup_area_id",
        F.coalesce(
            F.col("pickup_community_area"),
            F.lit(0)
        )
    )
    .withColumn(
        "dropoff_area_id",
        F.coalesce(
            F.col("dropoff_community_area"),
            F.lit(0)
        )
    )

    .groupBy(
        "trip_month",
        "pickup_area_id",
        "pickup_community_name",
        "dropoff_area_id",
        "dropoff_community_name"
    )

    .agg(
        # Route demand
        F.count("*").alias("total_trips"),

        # Financial metrics
        F.round(
            F.sum("trip_total"),
            2
        ).alias("total_trip_value"),

        F.round(
            F.avg("fare"),
            2
        ).alias("avg_fare"),

        F.round(
            F.avg("trip_total"),
            2
        ).alias("avg_trip_value"),

        # Trip characteristics
        F.round(
            F.avg("trip_miles"),
            2
        ).alias("avg_trip_miles"),

        F.round(
            F.avg("trip_duration_minutes"),
            2
        ).alias("avg_trip_duration_minutes"),

        F.round(
            F.avg("avg_speed_mph"),
            2
        ).alias("avg_speed_mph"),

        # Demand characteristics
        F.sum(
            F.when(
                F.col("is_peak_hour") == True,
                1
            ).otherwise(0)
        ).alias("peak_hour_trips"),

        F.sum(
            F.when(
                F.col("shared_trip_match") == True,
                1
            ).otherwise(0)
        ).alias("shared_trip_count")
    )

    .withColumn(
        "year",
        F.lit(2025)
    )

    .withColumnRenamed(
        "trip_month",
        "month"
    )
)

# COMMAND ----------

route_validation_df = route_metrics_df.agg(
    F.count("*").alias("route_metric_rows"),

    F.sum("total_trips").alias("reconciled_silver_trips"),

    F.min("month").alias("min_month"),

    F.max("month").alias("max_month")
)

display(route_validation_df)

# COMMAND ----------

# Gold table 5: Monthly executive metrics

monthly_metrics_df = (
    silver_df
    .groupBy("trip_month")
    .agg(
        F.count("*").alias("total_trips"),

        F.round(
            F.sum("trip_total"),
            2
        ).alias("total_trip_value"),

        F.round(
            F.sum("fare"),
            2
        ).alias("total_fare_amount"),

        F.round(
            F.sum("tip"),
            2
        ).alias("total_tip_amount"),

        F.round(
            F.avg("fare"),
            2
        ).alias("avg_fare"),

        F.round(
            F.avg("trip_total"),
            2
        ).alias("avg_trip_value"),

        F.round(
            F.avg("trip_miles"),
            2
        ).alias("avg_trip_miles"),

        F.round(
            F.avg("trip_duration_minutes"),
            2
        ).alias("avg_trip_duration_minutes"),

        F.round(
            F.avg("avg_speed_mph"),
            2
        ).alias("avg_speed_mph"),

        F.sum(
            F.when(
                F.col("is_peak_hour") == True,
                1
            ).otherwise(0)
        ).alias("peak_hour_trips"),

        F.sum(
            F.when(
                F.col("shared_trip_match") == True,
                1
            ).otherwise(0)
        ).alias("shared_trip_count"),

        F.sum(
            F.when(
                F.col("financial_data_missing") == True,
                1
            ).otherwise(0)
        ).alias("financial_data_missing_rows"),

        F.sum(
            F.when(
                F.col("pickup_geography_missing") == True,
                1
            ).otherwise(0)
        ).alias("pickup_geography_missing_rows"),

        F.sum(
            F.when(
                F.col("dropoff_geography_missing") == True,
                1
            ).otherwise(0)
        ).alias("dropoff_geography_missing_rows")
    )

    .withColumn(
        "year",
        F.lit(2025)
    )

    .withColumnRenamed(
        "trip_month",
        "month"
    )
    .orderBy("month")
)

# COMMAND ----------

monthly_validation_df = monthly_metrics_df.agg(
    F.count("*").alias("monthly_rows"),
    F.sum("total_trips").alias("reconciled_silver_trips"),
    F.min("month").alias("min_month"),
    F.max("month").alias("max_month")
)

display(monthly_validation_df)

# COMMAND ----------

gold_base_path = (
    "abfss://lakehouse@chicagoridedl.dfs.core.windows.net/gold/"
)

daily_metrics_path = gold_base_path + "daily_metrics/"
hourly_demand_path = gold_base_path + "hourly_demand/"
community_area_metrics_path = gold_base_path + "community_area_metrics/"
route_metrics_path = gold_base_path + "route_metrics/"
monthly_metrics_path = gold_base_path + "monthly_metrics/"

# COMMAND ----------

# Persist Gold datasets as Delta

(
    daily_metrics_df.write
    .format("delta")
    .mode("overwrite")
    .save(daily_metrics_path)
)

(
    hourly_demand_df.write
    .format("delta")
    .mode("overwrite")
    .save(hourly_demand_path)
)

(
    community_area_metrics_df.write
    .format("delta")
    .mode("overwrite")
    .save(community_area_metrics_path)
)

(
    route_metrics_df.write
    .format("delta")
    .mode("overwrite")
    .save(route_metrics_path)
)

(
    monthly_metrics_df.write
    .format("delta")
    .mode("overwrite")
    .save(monthly_metrics_path)
)

# COMMAND ----------

daily_written_df = spark.read.format("delta").load(daily_metrics_path)
hourly_written_df = spark.read.format("delta").load(hourly_demand_path)
community_written_df = spark.read.format("delta").load(community_area_metrics_path)
route_written_df = spark.read.format("delta").load(route_metrics_path)
monthly_written_df = spark.read.format("delta").load(monthly_metrics_path)

print("Daily metrics rows:", daily_written_df.count())
print("Hourly demand rows:", hourly_written_df.count())
print("Community metrics rows:", community_written_df.count())
print("Route metrics rows:", route_written_df.count())
print("Monthly metrics rows:", monthly_written_df.count())

# COMMAND ----------

gold_reconciliation = {
    "daily_metrics": daily_written_df.agg(
        F.sum("total_trips")
    ).first()[0],

    "hourly_demand": hourly_written_df.agg(
        F.sum("total_trips")
    ).first()[0],

    "community_pickup": community_written_df.agg(
        F.sum("pickup_trips")
    ).first()[0],

    "community_dropoff": community_written_df.agg(
        F.sum("dropoff_trips")
    ).first()[0],

    "route_metrics": route_written_df.agg(
        F.sum("total_trips")
    ).first()[0],

    "monthly_metrics": monthly_written_df.agg(
        F.sum("total_trips")
    ).first()[0]
}

for dataset, total in gold_reconciliation.items():
    print(f"{dataset}: {total:,}")