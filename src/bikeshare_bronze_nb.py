# Databricks notebook source
# Parameters - will be overridden by DAB base_parameters
dbutils.widgets.text("catalog", "OVERRIDE_ME")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("source_bucket", "s3://OVERRIDE_ME")
dbutils.widgets.text("volume_name", "raw_landing_zone")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
source_bucket = dbutils.widgets.get("source_bucket")
volume_name = dbutils.widgets.get("volume_name")
# COMMAND ----------

# Create catalog if it doesn't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")

# COMMAND ----------

# Create bronze schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze_schema}")

# COMMAND ----------

# Create external volume for raw data landing zone
spark.sql(f"""
    CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.{bronze_schema}.{volume_name}
    LOCATION '{source_bucket}/raw_landing_zone'
""")

# COMMAND ----------

# Read streaming data using Auto Loader
df = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{source_bucket}/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(f"/Volumes/{catalog}/{bronze_schema}/{volume_name}/dc_share_data/")
)

# COMMAND ----------

# Select only the columns we need for bronze table
df = df.select(
    "ride_id", 
    "rideable_type", 
    "started_at", 
    "ended_at", 
    "start_station_name", 
    "start_station_id", 
    "end_station_name", 
    "end_station_id", 
    "start_lat", 
    "start_lng", 
    "end_lat", 
    "end_lng", 
    "member_casual"
)

# COMMAND ----------

# Configure Spark to ignore missing files during streaming
# won't work with severless compute --> spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")

# COMMAND ----------

# Write stream to bronze Delta table
writeStream = (df.writeStream
    .format("delta")
    .option("checkpointLocation", f"{source_bucket}/_checkpoints/bronze")
    .trigger(availableNow=True)
    .outputMode("append")
    .toTable(f"{catalog}.{bronze_schema}.dc_rideshare_bt")
)