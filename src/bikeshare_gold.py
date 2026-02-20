# Databricks notebook source

# COMMAND ----------
dbutils.widgets.text("catalog", "OVERRIDE_ME")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.text("gold_schema", "gold")

catalog = dbutils.widgets.get("catalog")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ${catalog}.${gold_schema};
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.${gold_schema}.fact_rides_summary;

# COMMAND ----------
bikeshare_gold_df = spark.read.table(f"{catalog}.{silver_schema}.dc_rideshare_st")

# COMMAND ----------
from pyspark.sql import functions as F

bikeshare_gold_df = bikeshare_gold_df.withColumn("_data_quality_flag",
    F.when(
        (F.col("ride_duration_minutes") < 1) | (F.col("ride_duration_minutes") > 1440),
        F.lit("suspicious_duration")
    )
    .when(
        F.col("ride_distance_miles") / 1.6 > 50,
        F.lit("suspicious_distance")
    )
    .when(
        (F.col("trip_type") == "round_trip") & (F.col("ride_duration_minutes") < 5),
        F.lit("same_station_short")
    )
    .when(
        (F.col("ride_distance_miles") / (F.col("ride_duration_minutes") / 60)) > 30,
        F.lit("outlier_speed")
    )
    .when(
        F.col("start_station_name").isNull() | F.col("end_station_name").isNull(),
        F.lit("missing_station_info")
    )
    .otherwise(F.lit("valid"))
)

bikeshare_gold_df = (bikeshare_gold_df.drop("_rescued_data", "is_valid")
                 .withColumn("ride_date", F.to_date("started_at"))
                 .withColumn("ride_month", F.expr(f"`{catalog}`.`{gold_schema}`.month_name(ride_month)"))
                 .withColumnRenamed("member_casual", "user_type")
                 .withColumn("day_of_week", F.expr(f"`{catalog}`.`{gold_schema}`.day_of_week(day_of_week)")))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION ${catalog}.${gold_schema}.day_of_week(day int)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE 
# MAGIC   WHEN day = 1 THEN 'Monday'
# MAGIC   WHEN day = 2 THEN 'Tuesday'
# MAGIC   WHEN day = 3 THEN 'Wednesday'
# MAGIC   WHEN day = 4 THEN 'Thursday'
# MAGIC   WHEN day = 5 THEN 'Friday'
# MAGIC   WHEN day = 6 THEN 'Saturday'
# MAGIC   WHEN day = 7 THEN 'Sunday'
# MAGIC   ELSE 'Unknown'
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION ${catalog}.${gold_schema}.month_name(month int)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN month = 1 THEN 'January'
# MAGIC   WHEN month = 2 THEN 'February'
# MAGIC   WHEN month = 3 THEN 'March'
# MAGIC   WHEN month = 4 THEN 'April'
# MAGIC   WHEN month = 5 THEN 'May'
# MAGIC   WHEN month = 6 THEN 'June'
# MAGIC   WHEN month = 7 THEN 'July'
# MAGIC   WHEN month = 8 THEN 'August'
# MAGIC   WHEN month = 9 THEN 'September'
# MAGIC   WHEN month = 10 THEN 'October'
# MAGIC   WHEN month = 11 THEN 'November'
# MAGIC   WHEN month = 12 THEN 'December'
# MAGIC   ELSE 'Unknown'
# MAGIC END;

# COMMAND ----------

# create fact_rides_summary
fact_rides_df = bikeshare_gold_df

# COMMAND ----------

fact_rides_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog}.{gold_schema}.fact_rides_summary")

# COMMAND ----------

# create agg_station_metrics_daily

# Create DataFrame for rides STARTED at each station
df_starts = (bikeshare_gold_df
    .select(
        F.col("ride_date").alias("date"),
        F.col("start_station_id").alias("station_id"),
        F.col("start_station_name").alias("station_name"),
        F.col("start_lat").alias("station_lat"),
        F.col("start_lng").alias("station_lng"),
        F.lit("started").alias("event_type"),
        F.col("user_type"),
        F.col("rideable_type"),
        F.col("ride_duration_minutes"),
        F.col("ride_distance_miles")
    )
)

# Create DataFrame for rides ENDED at each station
df_ends = (bikeshare_gold_df
    .select(
        F.col("ride_date").alias("date"),
        F.col("end_station_id").alias("station_id"),
        F.col("end_station_name").alias("station_name"),
        F.col("end_lat").alias("station_lat"),
        F.col("end_lng").alias("station_lng"),
        F.lit("ended").alias("event_type"),
        F.col("user_type"),
        F.col("rideable_type"),
        F.col("ride_duration_minutes"),
        F.col("ride_distance_miles")
    )
)

# Union them together
df_all_events = df_starts.union(df_ends)

# Now aggregate by station
df_station_metrics = (df_all_events
    .groupBy("date", "station_id", "station_name", "station_lat", "station_lng")
    .agg(
        # Rides started
        F.sum(F.when(F.col("event_type") == "started", 1).otherwise(0)).alias("total_rides_started"),
        
        # Rides ended
        F.sum(F.when(F.col("event_type") == "ended", 1).otherwise(0)).alias("total_rides_ended"),
        
        # Member vs casual
        F.sum(F.when(F.col("user_type") == "member", 1).otherwise(0)).alias("member_rides"),
        F.sum(F.when(F.col("user_type") == "casual", 1).otherwise(0)).alias("casual_rides"),
        
        # Bike types (only count started rides to avoid double counting)
        F.sum(F.when((F.col("event_type") == "started") & (F.col("rideable_type") == "classic_bike"), 1).otherwise(0)).alias("classic_bike_rides"),
        F.sum(F.when((F.col("event_type") == "started") & (F.col("rideable_type") == "electric_bike"), 1).otherwise(0)).alias("electric_bike_rides"),
        F.sum(F.when((F.col("event_type") == "started") & (F.col("rideable_type") == "docked_bike"), 1).otherwise(0)).alias("docked_bike_rides"),
        
        # Duration and distance (only for started rides)
        F.avg(F.when(F.col("event_type") == "started", F.col("ride_duration_minutes"))).alias("avg_ride_duration_minutes"),
        F.sum(F.when(F.col("event_type") == "started", F.col("ride_duration_minutes")) / 60).alias("total_ride_duration_hours"),
        F.avg(F.when(F.col("event_type") == "started", F.col("ride_distance_miles"))).alias("avg_ride_distance_miles"),
        F.sum(F.when(F.col("event_type") == "started", F.col("ride_distance_miles"))).alias("total_ride_distance_miles")
    )
    .withColumn("net_bike_flow", F.col("total_rides_ended") - F.col("total_rides_started"))
    .withColumn("member_percentage", 
        F.round((F.col("member_rides") / (F.col("member_rides") + F.col("casual_rides"))) * 100, 1)
    )
)

df_station_metrics.write.mode("overwrite").saveAsTable(f"{catalog}.{gold_schema}.agg_station_metrics_daily")

# COMMAND ----------

user_behavior_df = bikeshare_gold_df
user_behavior_df = user_behavior_df.groupBy(
    F.year("ride_date").alias("year"),
    F.col("ride_month").alias("month"),
    F.col("user_type")
).agg(
    F.sum(F.when(F.col("user_type") == "member", 1).otherwise(0)).alias("member_rides"),
    F.sum(F.when(F.col("user_type") == "casual", 1).otherwise(0)).alias("casual_rides"),
    F.count("*").alias("total_rides"),
    F.mode("rideable_type").alias("most_popular_bike_type"),
    F.round(F.avg("ride_duration_minutes"), 2).alias("avg_ride_duration_minutes"),
    F.round(F.avg("ride_distance_miles"), 2).alias("avg_ride_distance_miles")
)

# COMMAND ----------

user_behavior_df.write.mode("overwrite").option("overwriteSchema","true").saveAsTable(f"{catalog}.{gold_schema}.user_behavior")
