"""
Glue ETL Job - Incident/Events Landing -> Clean

Transforms raw CHART incident/event JSON extracted from XML into a validated Parquet table.
The job: 

- Reads Firehose-written JSON lines from the landing zone. 
- Applies the incident schema (event_id, event_time, county, description, direction, lat/lon, lane status, traffic alert fields).
- Parses and normalizes timestamps, geometry, and enums. 
- Derives dt = yyyy-MM-dd for partitioning.
- Writes partitioned Parquet into the clean zone: 
    s3://tlms-clean-zone/incident-events/validated/dt=.../

This produces a reliable base for curated incident snapshots and daily summaries. 
"""

import sys
from pyspark.sql import functions as F, types as T
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Configuration
# Adjust bucket names if your real account uses -jtg etc.
LANDING_GLOB = "s3://tlms-landing-zone/incident-events/raw/*/*/*/*/*.gz"
CLEAN_PREFIX = "s3://tlms-clean-zone/incident-events/validated/"
MAX_ROWS = 200000


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
spark = SparkSession.builder.getOrCreate()
glue = GlueContext(spark.sparkContext)
job = Job(glue)
job.init(args["JOB_NAME"], args)

# Read landing JSONL (gz)
raw = spark.read.text(LANDING_GLOB)
print("DEBUG_RAW_LINE_COUNT =", raw.count())

# Schema matching lambda_incidents_pull_to_firehose output
inc_schema = T.StructType([
    T.StructField("ingest_ts",         T.LongType(),   True),
    T.StructField("event_id",          T.StringType(), True),
    T.StructField("create_time",       T.StringType(), True),
    T.StructField("start_time",        T.StringType(), True),
    T.StructField("closed",            T.BooleanType(), True),
    T.StructField("county",            T.StringType(), True),
    T.StructField("description",       T.StringType(), True),
    T.StructField("direction",         T.StringType(), True),
    T.StructField("incident_type",     T.StringType(), True),
    T.StructField("lat",               T.DoubleType(), True),
    T.StructField("lon",               T.DoubleType(), True),
    T.StructField("lanes_status",      T.StringType(), True),
    T.StructField("traffic_alert",     T.BooleanType(), True),
    T.StructField("traffic_alert_msg", T.StringType(), True)
])

parsed = raw.select(
    F.from_json(F.col("value"), inc_schema).alias("j")
)

df = parsed.select("j.*").where(F.col("j").isNotNull())

# Safety cap to avoid surprises
df = df.limit(MAX_ROWS)

#  Defensive casting / filling
for name, dtype in [
    ("ingest_ts",         T.LongType()),
    ("event_id",          T.StringType()),
    ("create_time",       T.StringType()),
    ("start_time",        T.StringType()),
    ("closed",            T.BooleanType()),
    ("county",            T.StringType()),
    ("description",       T.StringType()),
    ("direction",         T.StringType()),
    ("incident_type",     T.StringType()),
    ("lat",               T.DoubleType()),
    ("lon",               T.DoubleType()),
    ("lanes_status",      T.StringType()),
    ("traffic_alert",     T.BooleanType()),
    ("traffic_alert_msg", T.StringType()),
]:
    if name not in df.columns:
        df = df.withColumn(name, F.lit(None).cast(dtype))
    else:
        df = df.withColumn(name, F.col(name).cast(dtype))

#  3. Derive event_time and dt
# Parse start/create times as timestamps where possible
df = df.withColumn("start_ts",  F.to_timestamp(F.col("start_time")))
df = df.withColumn("create_ts", F.to_timestamp(F.col("create_time")))

# Prefer start_time, then create_time, then ingest_ts
df = df.withColumn(
    "event_time",
    F.coalesce(
        F.col("start_ts"),
        F.col("create_ts"),
        F.to_timestamp(F.from_unixtime(
            (F.col("ingest_ts") / 1000).cast("long")))
    )
)

df = df.drop("start_ts", "create_ts")

# dt as yyyy-MM-dd for partitioning (aligned with RWIS/WZDx)
df = df.withColumn(
    "dt",
    F.date_format(F.col("event_time"), "yyyy-MM-dd").cast(T.StringType())
)

# Normalize direction to uppercase (double safety even if Lambda already did)
df = df.withColumn(
    "direction",
    F.upper(F.col("direction"))
)

#  4. Select output columns
out_cols = [
    "event_id",
    "event_time",
    "dt",
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

df_out = df.select(*out_cols)

# Sanity logging
out_cnt = df_out.count()
print("DEBUG_OUT_COUNT =", out_cnt)
print(
    "DEBUG_DISTINCT_DT =",
    [r["dt"] for r in df_out.select("dt").distinct().limit(10).collect()]
)

if out_cnt == 0:
    raise RuntimeError(
        "Incidents clean produced 0 rows — check Lambda output and landing files.")

#  5. Write Parquet partitioned by dt
(
    df_out
    .write
    .mode("append")
    .partitionBy("dt")
    .parquet(CLEAN_PREFIX)
)

job.commit()
