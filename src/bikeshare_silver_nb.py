# Databricks notebook source
# COMMAND ----------
import bs_transformations
import subprocess
import sys
from pyspark.sql import functions as F
# COMMAND ----------
dbutils.widgets.text("whl_volume_path", "")
whl_volume_path = dbutils.widgets.get("whl_volume_path")
subprocess.check_call([sys.executable, "-m", "pip", "install", whl_volume_path, "--quiet"])

# COMMAND ----------
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

bikeshare_silver_df = spark.read.table(f"{catalog}.{bronze_schema}.dc_rideshare_bt")

# COMMAND ----------

"""
Remove Duplicates

- Deduplicate based on ride_id (primary key)
- Check for duplicate rides with identical start/end times and locations
"""
bikeshare_silver_df = bikeshare_silver_df.dropDuplicates(["ride_id"])

# COMMAND ----------

"""
Handle Null Values
Drop records where critical fields are null: ride_id, started_at, ended_at
For member_casual, either drop nulls or set a default value like "unknown"
Validate start_station_id, end_station_id are not null (or flag incomplete rides)
"""

check_valid_rides_df = bs_transformations.station_validation(bikeshare_silver_df)
check_valid_rides_df = check_valid_rides_df.filter(check_valid_rides_df.is_valid == 'True')
# COMMAND ----------

"""
Data Type Validation & Casting
Ensure started_at and ended_at are proper timestamp types
Ensure start_station_id and end_station_id are consistent integer types
"""
check_valid_rides_df = check_valid_rides_df.withColumn("started_at", F.col("started_at").cast("timestamp"))
check_valid_rides_df = check_valid_rides_df.withColumn("ended_at", F.col("ended_at").cast("timestamp"))
check_valid_rides_df = check_valid_rides_df.withColumn("start_station_id", F.col("start_station_id").cast("integer"))
check_valid_rides_df = check_valid_rides_df.withColumn("end_station_id", F.col("end_station_id").cast("integer"))

# COMMAND ----------

"""
Validate start_lat, start_lng, end_lat, end_lng are within valid ranges (DC coordinates)
"""
check_valid_rides_df = check_valid_rides_df.filter(F.col("start_lat").between(38.8, 39.9))
check_valid_rides_df = check_valid_rides_df.filter(F.col("end_lat").between(38.8, 39.9))
check_valid_rides_df = check_valid_rides_df.filter(F.col("start_lng").between(-77.2, -76.9))
check_valid_rides_df = check_valid_rides_df.filter(F.col("end_lng").between(-77.2, -76.9))

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
# need to extract the seconds property from the struct thats in the ride_duration F.column
bikeshare_silver_df = bikeshare_silver_df.withColumn("ride_duration (minutes)", F.timestamp_diff("MINUTE", F.F.col("started_at"), F.F.col("ended_at")))

# day of the week from started_at
bikeshare_silver_df = bikeshare_silver_df.withColumn("day_of_week", F.dayofweek(F.F.col("started_at")))
# hour of day
bikeshare_silver_df = bikeshare_silver_df.withColumn("hour_of_day", F.hour(F.F.col("started_at")))

# determine if day is weekedend
bikeshare_silver_df = bikeshare_silver_df.withColumn("is_weekend", F.when(F.F.col("day_of_week").isin([6,7]), True).otherwise(False))
# extract month and year from started_at
bikeshare_silver_df = bikeshare_silver_df.withColumn("ride_month", F.month(F.F.col("started_at")))
bikeshare_silver_df = bikeshare_silver_df.withColumn("ride_year", F.year(F.F.col("started_at")))
bikeshare_silver_df = bikeshare_silver_df.withColumnRenamed("ride_duration (minutes)", "ride_duration_minutes")

bikeshare_silver_df = bikeshare_silver_df.withColumn("trip_type", bs_transformations.classify_trip_type)

# COMMAND ----------

bikeshare_silver_df = bikeshare_silver_df.withColumn("ride_distance_km", bs_transformations.calculate_haversine_dist)
bikeshare_silver_df = bikeshare_silver_df.withColumn("ride_distance_km", bs_transformations.determine_trip_type)

# COMMAND ----------

"""
Business Rule Validation
Filter out rides where ended_at <= started_at (invalid times)
Remove rides with duration < 1 minute or > 24 hours (likely errors)
Flag or remove rides with impossible distances (e.g., > 50 miles for bikeshare)
"""
bikeshare_silver_df = bikeshare_silver_df.filter(F.F.col("ended_at") > F.F.col("started_at"))
bikeshare_silver_df = bikeshare_silver_df.filter((F.F.col("ride_duration_minutes") >= 1) & (F.F.col("ride_duration_minutes") <= 1440))
bikeshare_silver_df = bikeshare_silver_df.withColumn("ride_distance_km", F.round(F.F.col("ride_distance_km") / 1.609, 2))
bikeshare_silver_df = bikeshare_silver_df.withColumnRenamed("ride_distance_km", "ride_distance_miles")
bikeshare_silver_df = bikeshare_silver_df.filter(F.F.col("ride_distance_miles") <= 50)

# COMMAND ----------

bikeshare_silver_df = bikeshare_silver_df.select(*[F.trim(F.F.col(c[0])).alias(c[0]) if c[1] == 'string' else F.F.col(c[0]) for c in bikeshare_silver_df.dtypes])
bikeshare_silver_df = bikeshare_silver_df.withColumn("member_casual", F.lower(F.F.col("member_casual")))

# COMMAND ----------

bikeshare_silver_df = bikeshare_silver_df.withColumn("_ingestion_timestamp", F.current_timestamp())
bikeshare_silver_df = bikeshare_silver_df.withColumn("_source_file", F.F.col("_metadata.file_path"))

# COMMAND ----------
(
    bikeshare_silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("ride_month")
    .saveAsTable(f"{catalog}.{silver_schema}.dc_rideshare_st")

)