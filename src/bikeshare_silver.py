# Databricks notebook source
# COMMAND ----------
# Parameters - will be overridden by DAB base_parameters
dbutils.widgets.text("catalog", "OVERRIDE_ME")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("silver_schema", "silver")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")

# COMMAND ----------
# Create the silver schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

bikeshare_silver_df = spark.read.table(f"{catalog}.{bronze_schema}.dc_rideshare_bt")

# COMMAND ----------

"""
Remove Duplicates

- Deduplicate based on ride_id (primary key)
- Check for duplicate rides with identical start/end times and locations
"""
bikeshare_silver_df = bikeshare_silver_df.dropDuplicates(["ride_id"])

# COMMAND ----------

from pyspark.sql.functions import col, when
import pyspark.pandas as ps

def station_validation(df):
    validated_df = df.withColumn(
        "is_valid",
        when(
            (col("start_station_id").isNotNull()) &
            (col("end_station_id").isNotNull()), True

        ).otherwise(False)
    )
    return validated_df

# COMMAND ----------

"""
Handle Null Values
Drop records where critical fields are null: ride_id, started_at, ended_at
For member_casual, either drop nulls or set a default value like "unknown"
Validate start_station_id, end_station_id are not null (or flag incomplete rides)
"""

check_valid_rides_df = station_validation(bikeshare_silver_df)
check_valid_rides_df = check_valid_rides_df.where(check_valid_rides_df.is_valid == 'True')
# display(check_valid_rides_df)

# COMMAND ----------

"""
Data Type Validation & Casting
Ensure started_at and ended_at are proper timestamp types
Ensure start_station_id and end_station_id are consistent integer types
"""
check_valid_rides_df = check_valid_rides_df.withColumn("started_at", col("started_at").cast("timestamp"))
check_valid_rides_df = check_valid_rides_df.withColumn("ended_at", col("ended_at").cast("timestamp"))
check_valid_rides_df = check_valid_rides_df.withColumn("start_station_id", col("start_station_id").cast("integer"))
check_valid_rides_df = check_valid_rides_df.withColumn("end_station_id", col("end_station_id").cast("integer"))

# COMMAND ----------

"""
Validate start_lat, start_lng, end_lat, end_lng are within valid ranges (DC coordinates)
"""
check_valid_rides_df = check_valid_rides_df.filter(col("start_lat").between(38.8, 39.9))
check_valid_rides_df = check_valid_rides_df.filter(col("end_lat").between(38.8, 39.9))
check_valid_rides_df = check_valid_rides_df.filter(col("start_lng").between(-77.2, -76.9))
check_valid_rides_df = check_valid_rides_df.filter(col("end_lng").between(-77.2, -76.9))

# COMMAND ----------

"""
Calculate Derived Metrics
ride_duration: (ended_at - started_at) in minutes/seconds
ride_distance: Haversine distance between start/end coordinates
day_of_week: Extract from started_at
hour_of_day: Extract hour from started_at
is_weekend: Boolean flag
ride_month, ride_year: For partitioning
"""
# need to extract the seconds property from the struct thats in the ride_duration column
bikeshare_silver_df = bikeshare_silver_df.withColumn("ride_duration (minutes)", F.timestamp_diff("MINUTE", F.col("started_at"), F.col("ended_at")))

# day of the week from started_at
bikeshare_silver_df = bikeshare_silver_df.withColumn("day_of_week", F.dayofweek(F.col("started_at")))
# hour of day
bikeshare_silver_df = bikeshare_silver_df.withColumn("hour_of_day", F.hour(F.col("started_at")))

# determine if day is weekedend
bikeshare_silver_df = bikeshare_silver_df.withColumn("is_weekend", F.when(F.col("day_of_week").isin([6,7]), True).otherwise(False))
# extract month and year from started_at
bikeshare_silver_df = bikeshare_silver_df.withColumn("ride_month", F.month(F.col("started_at")))
bikeshare_silver_df = bikeshare_silver_df.withColumn("ride_year", F.year(F.col("started_at")))

# display(bikeshare_silver_df)

# COMMAND ----------

bikeshare_silver_df = bikeshare_silver_df.withColumnRenamed("ride_duration (minutes)", "ride_duration_minutes")

# COMMAND ----------

# Classify trip type
bikeshare_silver_df = bikeshare_silver_df.withColumn(
    "trip_type",
    F.when(
        (F.col("start_station_id") == F.col("end_station_id")) &
        (F.col("start_station_name") == F.col("end_station_name")),
        F.lit("round_trip")
    ).otherwise(F.lit("one_way"))
)

# COMMAND ----------

# Haversine distance calculation
bikeshare_silver_df = bikeshare_silver_df.withColumn(
    "ride_distance_km",
    F.round(F.acos(
        F.sin(F.radians(F.col("start_lat"))) * F.sin(F.radians(F.col("end_lat"))) +
        F.cos(F.radians(F.col("start_lat"))) * F.cos(F.radians(F.col("end_lat"))) *
        F.cos(F.radians(F.col("end_lng")) - F.radians(F.col("start_lng")))
    ) * 6371, 2)  # Earth's radius in kilometers
)

# For round trips, estimate based on duration (avg speed: 12 km/h)
bikeshare_silver_df = bikeshare_silver_df.withColumn(
    "ride_distance_km",
    F.when(
        (F.col("trip_type") == "round_trip") | (F.col("ride_distance_km") == 0),
        F.round((F.col("ride_duration_minutes") / 60) * 12, 2)
    ).otherwise(
        F.col("ride_distance_km")
    )
)

# COMMAND ----------

"""
Business Rule Validation
Filter out rides where ended_at <= started_at (invalid times)
Remove rides with duration < 1 minute or > 24 hours (likely errors)
Flag or remove rides with impossible distances (e.g., > 50 miles for bikeshare)
"""
bikeshare_silver_df = bikeshare_silver_df.filter(F.col("ended_at") > F.col("started_at"))
bikeshare_silver_df = bikeshare_silver_df.filter((F.col("ride_duration_minutes") >= 1) & (F.col("ride_duration_minutes") <= 1440))
bikeshare_silver_df = bikeshare_silver_df.withColumn("ride_distance_km", F.round(F.col("ride_distance_km") / 1.609, 2))
bikeshare_silver_df = bikeshare_silver_df.withColumnRenamed("ride_distance_km", "ride_distance_miles")
bikeshare_silver_df = bikeshare_silver_df.filter(F.col("ride_distance_miles") <= 50)

# COMMAND ----------

# bikeshare_silver_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardize values

# COMMAND ----------

# trim all columns
# only want to trim columns that are strings
bikeshare_silver_df = bikeshare_silver_df.select(*[F.trim(F.col(c[0])).alias(c[0]) if c[1] == 'string' else F.col(c[0]) for c in bikeshare_silver_df.dtypes])
bikeshare_silver_df = bikeshare_silver_df.withColumn("member_casual", F.lower(F.col("member_casual")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Optimization

# COMMAND ----------

"""_ingestion_timestamp: When record was processed to silver
_source_file: Track which bronze file this came from
"""
bikeshare_silver_df = bikeshare_silver_df.withColumn("_ingestion_timestamp", F.current_timestamp())
bikeshare_silver_df = bikeshare_silver_df.withColumn("_source_file", F.col("_metadata.file_path"))

# COMMAND ----------

# bikeshare_silver_df.count()

# COMMAND ----------

# bikeshare_silver_df.printSchema()

# COMMAND ----------

(
    bikeshare_silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("ride_month")
    .saveAsTable(f"{catalog}.{silver_schema}.dc_rideshare_st")

)
