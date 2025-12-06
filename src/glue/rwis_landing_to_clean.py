"""
Glue ETL Job - RWIS Landing -> Clean

Reads RWIS JSON lines written by Firehose into the S3 landing zone and converts them into validated, typed Parquet records. This job: 

- Applies an explicit schema for RWIS observations (station_id, event_time, temperature, wind, precipitation, surface status).
- Converts raw timestamps to event_time and derives dt = yyyy-MM-dd.
- Normalizes units (°F), clamps invalid enums, and handles missing values. 
- Writes partitioned Parquet output to the Clean zone:
    - s3://tlms-clean-zone/rwis/validated/dt=.../station_id=.../

This produces a consistent analytic foundation for RWIS hourly summaries and latest-per-station curated tables. 
"""

import sys
from pyspark.sql import functions as F, types as T
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# ===================== CONFIG =====================
# adjust if bucket name differs
LANDING_GLOB = "s3://tlms-landing-zone/rwis/raw/*/*/*/*/*.gz"
# adjust if bucket name differs
CLEAN_PREFIX = "s3://tlms-clean-zone/rwis/validated/"
MAX_ROWS = 200000
# ==================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
spark = SparkSession.builder.getOrCreate()
glue = GlueContext(spark.sparkContext)
job = Job(glue)
job.init(args["JOB_NAME"], args)

# Raw lines (gz JSONL)
raw = spark.read.text(LANDING_GLOB)
print("DEBUG_RAW_LINES=", raw.count())

# Explicit schema to match Lambda's normalized JSON
rwis_schema = T.StructType([
    T.StructField("station_id",       T.IntegerType(), True),
    T.StructField("ts",               T.LongType(),    True),  # epoch ms
    T.StructField("air_temp_f",       T.DoubleType(),  True),
    T.StructField("pavement_temp_f",  T.DoubleType(),  True),
    T.StructField("precip_type",      T.StringType(),  True),
    T.StructField("wind_mph",         T.DoubleType(),  True),
    T.StructField("surface_status",   T.StringType(),  True)
])

parsed = raw.select(F.from_json(F.col("value"), rwis_schema).alias("j"))
df = parsed.select("j.*").where(F.col("j").isNotNull())

# Cap
df = df.limit(MAX_ROWS)

# Ensure columns exist & cast types (defensive)
for name, dtype in [
    ("station_id",      T.IntegerType()),
    ("ts",              T.LongType()),
    ("air_temp_f",      T.DoubleType()),
    ("pavement_temp_f", T.DoubleType()),
    ("precip_type",     T.StringType()),
    ("wind_mph",        T.DoubleType()),
    ("surface_status",  T.StringType()),
]:
    if name not in df.columns:
        df = df.withColumn(name, F.lit(None).cast(dtype))
    else:
        df = df.withColumn(name, F.col(name).cast(dtype))

# Derive event_time and dt
df = df.withColumn(
    "event_time",
    F.to_timestamp(F.from_unixtime((F.col("ts")/1000).cast("long")))
)
df = df.withColumn(
    "event_time",
    F.when(F.col("event_time").isNull(), F.current_timestamp()
           ).otherwise(F.col("event_time"))
)
df = df.withColumn("dt", F.date_format(
    F.col("event_time"), "yyyy-MM-dd").cast(T.StringType()))

# Clamp enums
precip_allowed = F.array(
    [F.lit(v) for v in ["None", "Rain", "Snow", "Sleet", "Unknown"]])
surface_allowed = F.array([F.lit(v) for v in ["dry", "wet", "icy", "unknown"]])
df = df.withColumn(
    "precip_type",
    F.when(F.array_contains(precip_allowed, F.col("precip_type")),
           F.col("precip_type")).otherwise(F.lit("Unknown"))
).withColumn(
    "surface_status",
    F.when(F.array_contains(surface_allowed, F.col("surface_status")),
           F.col("surface_status")).otherwise(F.lit("unknown"))
)

# Select output
out_cols = [
    "station_id",
    "event_time",
    "dt",
    "air_temp_f",
    "pavement_temp_f",
    "precip_type",
    "wind_mph",
    "surface_status"
]
df_out = df.select(*out_cols)

# Sanity logs
print("DEBUG_NULL_TS=", df.filter(
    (F.col("ts").isNull()) | (F.col("ts") == 0)).count())
print("DEBUG_DISTINCT_DT=", [r["dt"] for r in df_out.select(
    "dt").distinct().limit(10).collect()])
out_cnt = df_out.count()
print("DEBUG_OUT_COUNT=", out_cnt)
if out_cnt == 0:
    raise RuntimeError(
        "Clean produced 0 rows — check Lambda output and landing files.")

# Write Parquet partitioned by dt, station_id
(df_out
 .write
 .mode("append")
 .partitionBy("dt", "station_id")
 .parquet(CLEAN_PREFIX)
 )

job.commit()
