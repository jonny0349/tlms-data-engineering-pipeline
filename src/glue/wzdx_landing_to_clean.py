"""
Glue ETL Job - WZDx Landing -> Clean

Processes flattened work-zone JSON lines from the landing zone and produces validated Parquet data. The job:

- Reads newline-delimited JSON written by the WZDx Lambda poller.
- Applies the WZDx clean schema (road_event_id, ingest_time, direction, coordinates, start_time, end_time).
- Normalizes strings, timestamps, and lists (e.g., road_names).
- Derives dt = yyyy-MM-dd for downstream partitioning. 
- Writes partitioned Parquet to the Clean zone: 
    - s3://tlms-clean-zone/wzdx/validated/dt=.../

This standardized structure supports curated current-events and daily summary tables
"""

import sys
from pyspark.sql import functions as F, types as T
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Configuration
LANDING_GLOB = "s3://tlms-landing-zone/wzdx/raw/*/*/*/*/*.gz"
CLEAN_PREFIX = "s3://tlms-clean-zone/wzdx/validated/"
MAX_ROWS = 200000

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
spark = SparkSession.builder.getOrCreate()
glue = GlueContext(spark.sparkContext)
job = Job(glue)
job.init(args["JOB_NAME"], args)

# 1. Read landing JSONL (gz)
raw = spark.read.text(LANDING_GLOB)
print("DEBUG_RAW_LINE_COUNT=", raw.count())

# Schema matching lambda_wzdx_pull_to_firehose output (top-level only)
wzdx_schema = T.StructType([
    T.StructField("ingest_ts",    T.LongType(),                  True),
    T.StructField("road_event_id", T.StringType(),                True),
    T.StructField("road_names",   T.ArrayType(T.StringType()),   True),
    T.StructField("direction",    T.StringType(),                True),
    T.StructField("start_date",   T.StringType(),                True),
    T.StructField("end_date",     T.StringType(),                True),
    T.StructField("begin_lat",    T.DoubleType(),                True),
    T.StructField("begin_lon",    T.DoubleType(),                True),
    T.StructField("end_lat",      T.DoubleType(),                True),
    T.StructField("end_lon",      T.DoubleType(),                True)
    # raw_feature is present in landing JSON but we intentionally ignore it here
])

parsed = raw.select(F.from_json(F.col("value"), wzdx_schema).alias("j"))
df = parsed.select("j.*").where(F.col("j").isNotNull())

# Safety cap
df = df.limit(MAX_ROWS)

# 2. Defensive casting
for name, dtype in [
    ("ingest_ts",    T.LongType()),
    ("road_event_id", T.StringType()),
    ("road_names",   T.ArrayType(T.StringType())),
    ("direction",    T.StringType()),
    ("start_date",   T.StringType()),
    ("end_date",     T.StringType()),
    ("begin_lat",    T.DoubleType()),
    ("begin_lon",    T.DoubleType()),
    ("end_lat",      T.DoubleType()),
    ("end_lon",      T.DoubleType()),
]:
    if name not in df.columns:
        df = df.withColumn(name, F.lit(None).cast(dtype))
    else:
        df = df.withColumn(name, F.col(name).cast(dtype))

# 3. Derive ingest_time, dt, start_time, end_time
df = df.withColumn("ingest_time", F.to_timestamp(
    F.from_unixtime((F.col("ingest_ts") / 1000).cast("long"))))

df = df.withColumn("ingest_time", F.when(F.col("ingest_time").isNull(
), F.current_timestamp()).otherwise(F.col("ingest_time")))

df = df.withColumn("dt", F.date_format(
    F.col("ingest_time"), "yyyy-MM-dd").cast(T.StringType()))

# Parse WZDx start/end time strings if possible
df = df.withColumn("start_time", F.to_timestamp(F.col("start_date")))
df = df.withColumn("end_time", F.to_timestamp(F.col("end_date")))

# Normalize direction (upper-case)
df = df.withColumn("direction", F.upper(F.col("direction")))

# 4. Select output columns
out_cols = [
    "road_event_id",
    "ingest_time",
    "dt",
    "start_time",
    "end_time",
    "road_names",
    "direction",
    "begin_lat",
    "begin_lon",
    "end_lat",
    "end_lon"
]
df_out = df.select(*out_cols)

# Sanity logs
out_cnt = df_out.count()
print("DEBUG_OUT_COUNT=", out_cnt)
print("DEBUG_DISTINCT_DT=", [r["dt"] for r in df_out.select(
    "dt").distinct().limit(10).collect()])
if out_cnt == 0:
    raise RuntimeError(
        "WZDx clean produced 0 rows - check Lambda output and landing files.")

# 5. Write Parquet partitioned by dt
(
    df_out
    .write
    .mode("append")
    .partitionBy("dt")
    .parquet(CLEAN_PREFIX)
)

job.commit()
