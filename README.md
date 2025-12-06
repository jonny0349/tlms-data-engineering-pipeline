# Traffic Lane Management System (TLMS) - Data Engineering Pipeline

This repository document the end-to-end cloud data pipeline built for the **Traffic Lane Management System (TLMS)** as part of a data engineering capstone project. The goal of the project is to integrate multiple transportation data sources (weather, work zones, and traffic incidents) into a unified, analytics-ready data platform on AWS.

The live implementation runs in AWS (S3, Lambda, Kinesis Firehose, Glue, Athena, etc.). This repo captures the **architecture, code, and SQL** used to build it so others can understand and reuse the design.

---

## 1. Problem & Objectives

Maryland's transportation data is rich but fragmented:

- **RWIS** (Road Weather Information System) stations publish weather and road-surface conditions.
- **WZDx** (Work Zone Data Exchange) publishes standardized work-zone information as GeoJSON.
- **CHART Incident/Events** rerport crashes, disabled vehicles, lane closures, and alerts.

These feeds:

- Use **different formats** (XML, JSON, GeoJSON, GZIP).
- Are **not aligned in time** or structure.
- Are **not stored together** in a way that makes cross-source analytics easy.

The TLMS pipeline was designed to:

1. **Continously ingest** RWIS, WZDx, and Incident/Events data into a centralized data lake .
2. **Standardize and clean** each feed into consistent schemas.
3. **Denormalize and aggregate** data into curated tables for analytics (e.g., daily summaries, current events snapshots).
4. Support **Athena queries** that can be used later in Tableau or any other visualization tool to answer three core questions:
   - Do the different data sources **line up** across dates?
   - How do **weather conditions** relate to **incidents**?
   - How is **incident performance by county** distributed?

---

## 2. High-Level Architecture

**Zone-based data lake design on AWS:**

- **Landing Zone (Raw)** - exact copies of source feeds
  - `s3://tlms-landing-zone/...`
- **Clean Zone (Validated)** - normalized, typed Parquet tables
  - `s3://tlms_clean_zone/...`
- **Curated Zone (analytics)** - denormalized and aggregated tables
  - `s3://tlms_curated_zone/...`

**Core AWS Services:**

- **AWS Lambda** - pulls external feeds (RWIS, WZDx, Incident/Events) and writes JSON lines to Firehose.
- **Amazon EventBridge** - schedules Lambda pollers (e.g., every 15 minutes).
- **Amazon Kinesis Firehose** - delivers streaming JSON to the Landing Zone in S3 (GZIP).
- **Amazon S3** - Storage for landing/clean/curated data.
- **AWS Glue Jobs (PySpark):** -
  - Landing -> Clean: Parse JSON/XML, enforce schema, basic cleansing.
  - Clean -> Curated: denormalization, aggregation, partitioning.
- **AWS Glue Crawlers** - Update the Glue Data Catalog with Parquet tables.
- **Amazon Athena** - query curated/clean tables using standard SQL.
- **AWS Systems Manager Parameter Store** - stores external API URLs and configuration.
- **AWS IAM** - roles and policies for Lambda, Firehose, Glue, Athena, and S3 access.

> Architecture diagrams and screenshots live in [`diagrams/`](diagrams/) (to be added).

---

## 3. Data Sources & Schemas

### 3.1 RWIS - Road Weather Information System

- **Source:** MDOT SHA RWIS feed (ArcGIS/JSON).
- **Ingestion:** Lambda -> Firehose -> S3 Landing (`rwis/raw/...`) every 15 minutes.
- **Clean schema (example):**

  - `station_id`
  - `event_time` (timestamp)
  - `dt` (date string `yyyy-MM-dd`)
  - `air_temp_f`, `pavement_temp_f`
  - `wind_mph`, `surface_status` (`dry/wet/icy/unknown`)
  - `precip_type`

- **Curated tables (examples):**
  - `tlms_curated_db.rwis_hourly_summary` - hourly per-station stats
  - `tlms_curated_db.rwis_latest_per_station` - latest snapshot per station.

### 3.2 WZDx - Work Zone Data Exchange

- **Source:** MDOT SHA WZDx v4.1 GeoJSON. `https://filter.ritis.org/wzdx_v4.1/mdot.geojson`
- **Ingestion:** Lambda -> Firehose -> S3 Landing (`wzdx/raw/...`) hourly.
- **Clean schema (example):**

  - `road_event_id`
  - `ingest_time`, `start_time`, `end_time`
  - `road_names`, `direction`,
  - `begin_lat`, `begin_lon`, `end_lat, `end_lon`

- **Curated tables (examples):**
  - `tlms_curated_db.wzdx_current_events` - latest state per `road_event_id`
  - `tlms_curated_db.wzdx_daily_summary` - daily counts and durations by `dt` and `direction`.

### 3.3 CHART Incident/Events

- **Source:** CHART incident/events feed (XML)
- **Ingestion:** Lambda -> Firehose -> S3 Landing (`incident-events/raw/...`) every 30 minutes.
- **Clean schema (example):**

  - `event_id`
  - `event_time` (timestamp), `dt` (date)
  - `county`, `description`, `direction`, `incident_type`
  - `lat`, `lon`
  - `lanes_status`
  - `traffic_alert` (boolean), `traffic_alert_msg`

- **Curated tables (examples):**
  - `tlms_curated_db.inc_current_events` - latest record per `event_id`
  - `tlms_curated_db.wzdx_daily_summary` - daily incident counts by `dt`, `county`, `direction`

---

## 4. Pipeline Walkthrough (Per Dataset)

At a high level, each dataset follows the same pattern:

1. **Ingest:**

   - EventBridge triggers a Lambda poller on a schedule.
   - Lambda reads an external endpoint (JSON/GeoJSON/XML), flattens it, and writes JSON lines to a Firehose delivery stream
   - Firehose writes compressed JSON (`.gz`) to the **Landing Zone** in S3

2. **Landing -> Clean (Glue Job):**

   - Read raw JSON/XML from landing.
   - Apply an explicit schema (PySpark/Glue).
   - Parse and normalize fields (types, enums, basic cleansing).
   - Write **Parquet** to the Clean Zone, partitioned by `dt` (and sometimes `station_id`).

3. **Clean -> Curated (Glue Job):**

   - RWIS: build hourly summaries + latest per station.
   - WZDx: build current work zones + daily summaries.
   - Incident/Events: build current incidents + daily summaries.
   - Output denormalized tables to the Curated Zone (Parquet, partitioned).

4. **Catalog & Query**
   - Glue Crawlers update the Glue Data Catalog for clean and curated datasets.
   - Athena queries join across RWIS, WZDx, and Incident/Events to power analysis.

> Detailed transformation notes and screenshots are in [`docs/transformations.md`](docs/transformations.md) (to be added).

---

## 5. Analytics & Example Queries

We wrote several Athena queries to answer the three main project questions. Example themes:

1. **Daily multi-source coverage**
   - Compare, by `dt`, how many RWIS observations, WZDx work zones, and Incident/Events are recorded.
2. **Weather vs Incidents**
   - Relate daily incident counts to average air temperature and icy/wet/dry RWIS observations.
3. ##Incident performance by county\*\*
   - Rank counties by incident volume and alert incidents, and dicuss data quality issues (e.g., missing or unreliable closure flags).

SQL examples live in [`sql/`](sql/), organized by problem:

- `sql/problem1_daily_coverage.sql`
- `sql/problem2_weather_vs_incidents.sql`
- `sql/problem3_incident_performance_by_county.sql`

---

## 6. Repository Structure

Planned structure for this repo:

```text
.
├── README.md                     # Project overview (this file)
├── diagrams/                     # Architecture & pipeline diagrams (PNG + .drawio)
├── docs/
│   ├── problem_statement.md      # Business problems & requirements
│   ├── architecture.md           # Detailed AWS architecture & design choices
│   └── transformations.md        # RWIS, WZDx, Incidents transformation details
├── src/
│   ├── lambda/                   # Lambda pollers for RWIS, WZDx, Incidents
│   └── glue/                     # PySpark/Glue ETL jobs (landing→clean, clean→curated)
├── sql/                          # Athena queries for analysis & dashboards
└── infra/
    ├── iam_policies/             # Example IAM JSON policies (sanitized)
    └── notes.md                  # Notes on roles, crawlers, triggers, schedules

```

## 7. How to Reuse this Design

To adapt this pipeline in your own AWS account:

1. Crate three S3 buckets for **landing, clean, and curated** zones
2. Implement Firehose delivery streams pointing to the landing bucket.
3. Deploy Lambda functions from src/lambda/ and write them to:
   - EventBridge schedules
   - Parameter store for API URLs
   - Firehose delivery streams
4. Create Glue Jobs from src/glue for:
   - Landing -> Clean (JSON/XML -> Parquet)
   - Clean -> Curated (denormalization and aggregation)
5. Configure Glue Crawlers to register clean and curated Parquet datasets.
6. Use the SQL in sql/ to query your data with Athena and connect to BI tools (e.g., Tableau).

## 8. Future Work

Potential extensions to this project include:

- Adding more data sources (e.g., travel times, speed sensors, probe data).
- Enhancing geospatial analytics with Athena/Trino or specialized tools.
- Building live dashboards for operations teams.
- Integrating ML models (e.g., incident risk prediction based on weather + work zones).
- Hardening infrastructure with IaC (Terraform/CDK), CI/CD, and automated testing.

---

**NOTE:** If you are reading this as a reviewer or potential employer and want to understand specific parts of the pipeline (e.g., Glue jobs, Lambda patterns, or Athena queries), check out the src/, docs/, and sql/ folders, or open an issue in this repo.
