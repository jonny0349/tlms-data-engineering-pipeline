"""
Glue ETL Job - RWIS Clean -> Curted

Build curated analytics tables from clean RWIS observations. The job:

1. Creates an hourly per-station summary:
    - Aggregates temperatures, pavement conditions, wind speeds, and surface_status counts. 
    - Produces one row per (dt, station_id, event_hour).

2. Creates a "latest per station" snapshot: 
    - Uses a window function to pick the most recent reading for each station. 
    - Produces a denormalized row containing current conditins.

Outputs:
    - s3://tlms-curated-zone/rwis/hourly_summary/
    - s3://tlms-curated-zone/rwis/latest_per_station/

These curated tables support cross-dataset joins (RWIS <-> WZDx <-> Incident/Events) and daily operational analytics. 
"""

import sys
from pyspark.sql import functions as F, types as T, Window
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# CONFIG
CLEAN_DB = "tlms_clean_db"
CLEAN_TABLE = "rwis_validated"         # check exact name in Glue Catalog
CURATED_BUCKET = "tlms-curated-zone"      # adjust if different
HOURLY_PREFIX = f"s3://{CURATED_BUCKET}/rwis/hourly_summary/"
LATEST_PREFIX = f"s3://{CURATED_BUCKET}/rwis/latest_per_station/"


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
spark = SparkSession.builder.getOrCreate()
glue = GlueContext(spark.sparkContext)
job = Job(glue)
job.init(args["JOB_NAME"], args)

# Read clean table
df_clean = spark.table(f"{CLEAN_DB}.{CLEAN_TABLE}")

# Ensure dt is STRING and drop nulls
df_clean = df_clean.where(F.col("dt").isNotNull())
df_clean = df_clean.withColumn("dt", F.col("dt").cast(T.StringType()))

#  Hourly summary
df_hourly = (
    df_clean
    .withColumn("event_hour", F.date_trunc("hour", F.col("event_time")))
)

df_hourly_agg = (
    df_hourly
    .groupBy("dt", "station_id", "event_hour")
    .agg(
        F.avg("air_temp_f").alias("avg_air_temp_f"),
        F.avg("pavement_temp_f").alias("avg_pavement_temp_f"),
        F.avg("wind_mph").alias("avg_wind_mph"),
        F.sum(F.when(F.col("surface_status") == "dry",
              1).otherwise(0)).alias("cnt_dry"),
        F.sum(F.when(F.col("surface_status") == "wet",
              1).otherwise(0)).alias("cnt_wet"),
        F.sum(F.when(F.col("surface_status") == "icy",
              1).otherwise(0)).alias("cnt_icy"),
        F.count(F.lit(1)).alias("obs_count")
    )
)

print("DEBUG_HOURLY_COUNT=", df_hourly_agg.count())

(
    df_hourly_agg
    .write
    .mode("append")
    .partitionBy("dt", "station_id")
    .parquet(HOURLY_PREFIX)
)

#  Latest per station snapshot
w = Window.partitionBy("station_id").orderBy(F.col("event_time").desc())

df_latest = (
    df_clean
    .withColumn("rn", F.row_number().over(w))
    .where(F.col("rn") == 1)
    .drop("rn")
    .withColumn("snapshot_dt", F.col("dt"))
)

print("DEBUG_LATEST_COUNT=", df_latest.count())

(
    df_latest
    .write
    .mode("append")
    .partitionBy("snapshot_dt")
    .parquet(LATEST_PREFIX)
)

job.commit()
