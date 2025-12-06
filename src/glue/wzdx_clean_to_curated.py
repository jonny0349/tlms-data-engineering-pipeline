"""
Glue ETL job: Clean -> Curated for WZDx work zone data. 
Inputs:
    - tlms_clean_db.wzdx_validated (Parquet in the Clean Zone)

Outputs: 
    - s3://tlms-curated-zone/wzdx/current_events/
        -> one "current" row per road_event_id (latest ingest)
    - S3://tlms-curated-zone/wzdx/daily_summary/
        -> daily summary by dt and direction (counts, duration, latest ingest)

Registered in Glue Catalog as:
    - tlms_curated_db.wzdx_current_events
    - tlms_curated_db.wzdx_daily_summary
"""

import sys
from pyspark.sql import functions as F, types as T, Window
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Configuration
CLEAN_DB = "tlms_clean_db"
CLEAN_TABLE = "wzdx_validated"         # check exact table name in Glue Catalog
CURATED_BUCKET = "tlms-curated-zone"

CURRENT_PREFIX = f"s3://{CURATED_BUCKET}/wzdx/current_events/"
DAILY_PREFIX = f"s3://{CURATED_BUCKET}/wzdx/daily_summary/"


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
spark = SparkSession.builder.getOrCreate()
glue = GlueContext(spark.sparkContext)
job = Job(glue)
job.init(args["JOB_NAME"], args)

# 1. Read clean WZDx table
df_clean = spark.table(f"{CLEAN_DB}.{CLEAN_TABLE}")

# Ensure dt is STRING and not null
df_clean = df_clean.where(F.col("dt").isNotNull())
df_clean = df_clean.withColumn("dt", F.col("dt").cast(T.StringType()))

print("DEBUG_CLEAN_COUNT=", df_clean.count())

#  2. Current active work zones (latest per road_event_id)
# If road_event_id is null, we can still keep those as separate records.
w = Window.partitionBy("road_event_id").orderBy(F.col("ingest_time").desc())

df_latest = (
    df_clean
    .withColumn("rn", F.row_number().over(w))
    .where(F.col("rn") == 1)
    .drop("rn")
)

# Snapshot date for partitioning
df_latest = df_latest.withColumn(
    "snapshot_dt",
    F.date_format(F.col("ingest_time"), "yyyy-MM-dd").cast(T.StringType())
)

# Select & order columns for curated "current events"
latest_cols = [
    "snapshot_dt",
    "road_event_id",
    "ingest_time",
    "start_time",
    "end_time",
    "road_names",
    "direction",
    "begin_lat",
    "begin_lon",
    "end_lat",
    "end_lon"
]
df_latest_out = df_latest.select(*latest_cols)

print("DEBUG_LATEST_COUNT=", df_latest_out.count())

(
    df_latest_out
    .write
    .mode("append")
    .partitionBy("snapshot_dt")
    .parquet(CURRENT_PREFIX)
)

#  3. Daily summary of work zones
# Approximate duration in hours (if both start & end exist)
df_dur = df_clean.withColumn(
    "duration_hours",
    (F.col("end_time").cast("long") - F.col("start_time").cast("long")) / 3600.0
)

df_daily = (
    df_dur
    .groupBy("dt", "direction")
    .agg(
        F.countDistinct("road_event_id").alias("work_zone_count"),
        F.min("start_time").alias("min_start_time"),
        F.max("end_time").alias("max_end_time"),
        F.avg("duration_hours").alias("avg_duration_hours"),
        F.max("ingest_time").alias("latest_ingest_time")
    )
)

# Ensure direction is string
df_daily = df_daily.withColumn(
    "direction", F.col("direction").cast(T.StringType()))

print("DEBUG_DAILY_COUNT=", df_daily.count())

(
    df_daily
    .write
    .mode("append")
    .partitionBy("dt")
    .parquet(DAILY_PREFIX)
)

job.commit()
