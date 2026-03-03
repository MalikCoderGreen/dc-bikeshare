# Databricks notebook source

 # COMMAND ----------
import subprocess
import sys
dqx_whl_volume_path = dbutils.widgets.get("dqx_whl_volume_path")
subprocess.check_call([sys.executable, "-m", "pip", "install", dqx_whl_volume_path, "--quiet"])

 # COMMAND ----------
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import VolumeFileChecksStorageConfig
from databricks.sdk import WorkspaceClient

# COMMAND ----------
dbutils.widgets.text("catalog", "OVERRIDE_ME")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("source_bucket", "s3://OVERRIDE_ME")
dbutils.widgets.text("volume_name", "raw_landing_zone")
dbutils.widgets.text("dqx_whl_volume_path", "OVERRIDE_ME")


# COMMAND ----------
catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
source_bucket = dbutils.widgets.get("source_bucket")
volume_name = dbutils.widgets.get("volume_name")
# COMMAND ----------

# Create catalog if it doesn't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze_schema}")

# COMMAND ----------
spark.sql(f"""
    CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.{bronze_schema}.{volume_name}
    LOCATION '{source_bucket}/raw_landing_zone'
""")

# COMMAND ----------
df = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"/Volumes/{catalog}/{bronze_schema}/{volume_name}/dc_share_data/")
)

# COMMAND ----------
ws = WorkspaceClient()
dq_engine = DQEngine(ws)
# COMMAND ----------
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
df_quality_checks = dq_engine.load_checks(config=VolumeFileChecksStorageConfig(location=f"/Volumes/{catalog}/config_files/dqx_files/dqx_checks.yaml"))
# COMMAND ----------
valid_df, quarantined_df = dq_engine.apply_checks_by_metadata_and_split(df, df_quality_checks)
# COMMAND ----------
display(valid_df)
# COMMAND ----------
display(quarantined_df)
# COMMAND ----------
valid_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{bronze_schema}.dc_rideshare_bt")

# COMMAND ----------
quarantined_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{bronze_schema}.dc_rideshare_quarantined")