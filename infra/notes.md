# Infrastructure Notes - TLMS Data Engineering Pipeline

This docuent summarizes how the AWS services in this project are wired together.
It is not full IaC (Terraform/CloudFormation), but a high-level guide to recreate the pipeline in another AWS account.

---

## 1. S3 Buckets and Zones

The pipeline uses a classic three-zone data lake layout in Amazon S3:

- **Landing Zone** - raw, as-ingested data

  - `s3://tlms-landing-zone/`
  - Prefixes:
    - `rwis/raw/yyyy/MM/dd/...`
    - `wzdx/raw/yyyy/MM/dd/...`
    - `incident-events/raw/yyyy/MM/dd/...`

- **Clean Zone** - validated, typed Parquet

  - `s3://tlms-clean-zone`
  - Prefixes:
    - `rwis/validated/dt=.../station_id=.../`
    - `wzdx/validated/dt=.../`
    - `incident-events/validated/dt=.../`

- **Curated Zone** - denormalized + aggregated Parquet
  - `s3://tlms-curated-zone`
  - Prefixes:
    - `rwis/hourly_summary/`
    - `rwis/latest_per_station/`
    - `wzdx/current_events/`
    - `wzdx/daily_summary/`
    - `incident-events/current_events/`
    - `incident-events/daily_summary/`

In the real project, bucket names may include a suffix with name initials to ensure uniqueness.

---

## 2. AWS System Manager Parameter Store

External URLs and configuration are stored in **Parameter Store** so Lambda code does not hardcode endpoints.

Example parameters:

- RWIS

  - `tlms/rwis/api_url` - [RWIS API URL]
  - `tlms/rwis/center_lat`, `tlms/rwis/center_lon`, `tlms/rwis/radius_km` (added an optional geofence to not pull a lot of data for testing purposes. It can be removed if the needs is to pull large quantities of data).

- WZDx

  - `tlms/wzdx/api_url` - [MDOT WZDx GeoJSON URL]

- Incident/Events
  - `tlms/incidents/api_url` - [CHART Incident XML URL]

Lambda functions use `boto3.client("ssm")` to read these values at runtime. All of these datasets are publicly available and can be found on https://chart.maryland.gov/datafeeds/getdatafeeds

---

## 3. IAM Roles and Policies (Conceptual)

### 3.1 Lambda Poller Role

Example role: `lambda_tlms_poller_role`

- **Trusted service:** `lambda.amazonaws.com`
- **Key permissions:**
  - `ssm:GetParameter` on `tlms/*` parameters
  - `firehose:PutRecord` and `firehose:PutRecordBatch` on:
    - `rwis_firehose_to_s3_tlms`
    - `wzdx-firehose-to-s3-tlms`
    - `incidents-firehose-to-s3-tlms`
  - Basic logging:
    - `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`

Attached policies (conceptually):

- `LambdaRWIS_Firehose_SSM_TLMS` (or similar consolidated policy)
- `AWSLambdaBasicExecutionRole`

Each Lambda (RWIS, WZDx, Incient/Events) uses this role or a similar one.

---

### 3.2 Firehose delivery role

Example role: `firehose_delivery_role_tlms`

- **Trusted service:** `firehose.amazonaws.com`
- **Key permissions:**
  - `s3:PutObject`, `s3:AbortMultipartUpload`, `s3:GetBucketLocation`, `s3:ListBucket`, `s3:ListBucketMultipartUploads` on the landing bucket
  - Optional loggin to CloudWatch Logs / S3 error prefix

Each Firehose stream (RWIS, WZDx, Incident/Events) uses this role to write `.gz` files to the landing zone.

---

### 3.3 Glue job / crawler role

Example role: `glue_job_role_tlms`

- **Trusted service:** `glue.amazonaws.com`
- **Key permissions:**
  - Read from Landing zone:
    - `s3:GetObject`, `s3:ListBucket` on `tlms-landing-zone/*`
  - Read/Write Clean zone:
    - `s3:GetObject`, `s3:PutObject`, `s3:ListBucket`, on `tlms-clean-zone/*`
  - Read/Write Curated zone:
    `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on `tlms-curated-zone/*`
  - Glue Catalog:
    - `glue:GetDatabase`, `glue:GetTable`, `glue:CreateTable`, `glue:UpdateTable`, `glue:GetCrawler`, `glue:StartCrawler`
  - Logging:
    - `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`

This single role can be reused by:

- Landing -> Clean Glue jobs
- Clean -> Curated Glue jobs
- Glue Crawlers

---

## 4. EventBridge Rules (Scheduler)

Each Lambda poller is triggered by an **EventBridge rule** on a schedule.

Examples:

- **RWIS rule**

  - Name: `rwis_poller_schedule`
  - Schedule: `rate (15 minutes)`
  - Target: RWIS Lambda function (e.g., `rwis_poller`)

- **WZDx rule**

  - Name: `wzdx_poller_schedule`
  - Schedule: `rate (15 minutes)`
  - Target: WZDx Lambda function

- **Incident/Events rule**
  - Name: `incidents_poller_schedule`
  - Schedule: `rate (15 minutes)`
  - Target: Incidents Lambda function

The rule input is typically a small JSON object (or empty) - the Lambda uses Parameter Store for configuration, not rule payload.

## 5. Kinesis Firehose Deliver Streams

Three Firehose delivery streams are used to buffer and batch data into the Landing zone:

- `rwis_firehose_to_s3_tlms`
- `wzdx_firehose_to_s3_tlms`
- `incidents_firehose_to_s3_tlms`

Key configurations:

- **Source:** Direct `PutRecord/PutRecordBatch` from Lambda.
- **Destination:** S3 Landing zone bucket + dataset-specific prefix.
- **Buffering:** Time-based (e.g., 60 seconds) and/or size-based (e.g., 1-5 MB).
- **Compression:** GZIP.
- **IAM Role:** `firehose_delivery_role_tlms`

---

## 6. Glue Jobs and Crawlers

### 6.1 Glue Jobs

For each dataset, there are two Glue ETL jobs implemented in `src/glue/`:

- `*_landing_to_clean.py` - reads Landing zone raw data, writes validated Parquet to clean zone.
- `*_clean_to_curated.py` - reads clean Parquet, writes denormalized/aggregated Parquet to curated zone.

Jobs are scheduled (via Glue triggers) to run shortly after new data arrives, for example:

- RWIS, WZDX, Incident/Events:
  - Landing -> Clean every 30 minutes
  - Clean -> Curated after landing -> clean is in a `succeeded` state

### 6.2 Crawlers

Glue Crawlers register tables for Clean and Curated zones:

- Clean database: `tlms_clean_db`
  - Tables: `rwis_validated`, `wzdx_validated`, `incident/validated`, etc.
- Curated database: `tlms_curated_db`
  - Tables: `rwis_hourly_summary`, `rwis_latest_per_station`, `wzdx_current_events`, `wzdx_daily_summary`, `inc_current_events`, `inc_daily_summary`, etc.

Crawlers can be schedule daily (e.g., 2:00 a.m.) or run on deman. The project has the crawlers running daily at 2 a.m.

---

## 7. Query & Visualization

- **Amazon Athena** uses the Glue Data Catalog to query the curated tables.
- SQL queries used in this project are stored in the [`sql/`](/sql/) directory.
- Query results are exported to CSV and visualized in Tableau to answer the three core TLMS questions (multi-srouce coverage, weather vs incidents, and incident performance by county).

---

This overview, together with the code in `src/` and documentation in `docs/`, is sufficient to recreate the TLMS data pipeline in an AWS account wiht similar services and permissions.
