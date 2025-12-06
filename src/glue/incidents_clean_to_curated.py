"""
Glue ETL Job - Incident/Events Clean -> Curated

Creates curated tables for Incident/Events analytics. 

1. Current Incidents:
    - Uses a window function to select the latest record per event_id
    - Produces a denormalized "current state" table including county, description, direction, traffic alert flags, and coordinates. 

2. Daily summary:
    - Aggregates by dt, county, and direction. 
    - Computes total_incidents, open_incidents, closed_incidents (if available), alert_incidents, and latest_event_time

Outputs: 
    - s3://tlms-curated-zone/incident-events/current_events/
    - s3://tlms-curated-zone/incident-events/daily_summary/

These curated datasets support county-level performance analytics and weather/work-zone correlation studies. 
"""

import sys
from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Configuration
CLEAN_TABLE = "tlms_clean_db.incident_validated"
CURR_PREFIX = "s3://tlms-curated-zone-jtg/incident-events/current_events/"
DAILY_PREFIX = "s3://tlms-curated-zone-jtg/incident-events/daily_summary/"


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
spark = SparkSession.builder.getOrCreate()
glue = GlueContext(spark.sparkContext)
job = Job(glue)
job.init(args["JOB_NAME"], args)

# Read clean incidents table
df = spark.read.table(CLEAN_TABLE)

# Just in case: ensure types
df = (
    df
    .withColumn("event_time", F.col("event_time").cast("timestamp"))
    .withColumn("dt", F.col("dt").cast("string"))
    .withColumn("direction", F.upper(F.col("direction")))
)

# Filter out rows without event_id or event_time
df = df.where(F.col("event_id").isNotNull() & F.col("event_time").isNotNull())

# Curated: incidents_current
# Latest record per event_id based on event_time (ties broken by ingest_time if present)
w = Window.partitionBy("event_id").orderBy(F.col("event_time").desc())

df_cur = (
    df
    .withColumn("rn", F.row_number().over(w))
    .where(F.col("rn") == 1)
    .drop("rn")
)

# Snapshot date (partition)
df_cur = df_cur.withColumn(
    "snapshot_dt",
    F.date_format(F.col("event_time"), "yyyy-MM-dd")
)

cur_cols = [
    "event_id",
    "snapshot_dt",
    "event_time",
    "closed",
    "county",
    "description",
    "direction",
    "incident_type",
    "lat",
    "lon",
    "lanes_status",
    "traffic_alert",
    "traffic_alert_msg"
]

df_cur_out = df_cur.select(*cur_cols)

# Write to curated/current_events partitioned by snapshot_dt
(
    df_cur_out
    .write
    .mode("overwrite")
    .partitionBy("snapshot_dt")
    .parquet(CURR_PREFIX)
)

# Curated: incidents_daily_summary
df_days = df_cur_out.withColumn(
    "dt",
    F.date_format(F.col("event_time"), "yyyy-MM-dd")
)

# Aggregation by dt, county, direction
df_daily = (
    df_days
    .groupBy("dt", "county", "direction")
    .agg(
        F.countDistinct("event_id").alias("total_incidents"),
        F.sum(F.when(F.col("closed") == False, 1).otherwise(
            0)).alias("open_incidents"),
        F.sum(F.when(F.col("closed") == True, 1).otherwise(
            0)).alias("closed_incidents"),
        F.sum(F.when(F.col("traffic_alert") == True,
              1).otherwise(0)).alias("alert_incidents"),
        F.max("event_time").alias("latest_event_time")
    )
)

# Write to curated/daily_summary partitioned by dt
(
    df_daily
    .write
    .mode("overwrite")
    .partitionBy("dt")
    .parquet(DAILY_PREFIX)
)

job.commit()
