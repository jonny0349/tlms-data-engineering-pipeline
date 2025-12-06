# TLMS Data Engineering Architecture

This document describes the cloud architecture used to ingest, transform, and curate three transportation data sources for the **Traffic Lane Management System (TLMS):**

- RWIS: Road Weather Information System (road weather and pavement conditions)
- WZDx: Work Zone Data Exchange (active work zones as GeoJSON)
- CHART Incident/Events: crashes, lane closures, traffic alerts.

The pipeline is implemented on **AWS** using a **Landing -> Clean -> Curated** data lake pattern.

---

## 1. High-Level Design

At a high level, the TLMS pipeline:

1. **Ingests** external feeds on a schedule using **EventBridge + Lambda + Kinesis Firehose**.
2. Stores **raw copies** of each feed in an **S3 Landing Zone**.
3. Uses **Glue ETL jobs (PySpark)** to:
   - parse JSON/GeoJSON/XML,
   - enforce schemas,
   - clean and normalize records,
   - and write **Parquet** to a **Clean Zone**.
4. Uses additional **Glue ETL jobs** to:
   - denormalize and aggregate clean data,
   - build curated "current state" and daily summary tables in a **Curated Zone**.
5. Registers clean and curated tables in the **Glue Data Catalog** via **Glue Crawlers**.
6. Exposes data to **Athena** for SQL queries that can be used for visualizations in Tableau or any other BI tools.

Buckets follow a three-zone structure:

- `tlms_landing_zone/...`
- `tlms_clean_zone/...`
- `tlms_curated_zone/...`

---

## 2. Core AWS Services and Their Roles

### 2.1 Storage - Amazon S3

- **Landing Zone**

  - Stores **raw** ingested data exactly as received (JSON, GeoJSON, XML, GZIP).
  - Example prefixes:
    - `rwis/raw/yyyy/MM/dd`
    - `wzdx/raw/yyyy/MM/dd`
    - `incident-events/raw/yyyy/MM/dd`

- **Clean Zone**

  - Stores **validated, typed Parquet** tables.
  - Data is partitioned (e.g., by `dt`, sometimes `station_id`).
  - Example prefixes:
    - `rwis/validated/dt=.../station_id=.../`
    - `wzdx/validated/dt=.../`
    - `incident-events/current_events/` and `incident-events/daily_summary/`

- **Curated Zone**
  - Stores **denormalized and aggregated** tables tuned for analytics.
  - Example prefixes:
    - `rwis/hourly_summary/` and `rwis/latest_per_station/`
    - `wzdx/current_events/` and `wzdx/daily_summary/`
    - `incident-events/current_events/` and `incident-events/daily_summary/`

---

### 2.1 Ingestion - EventBridge, Lambda, Firehose, Parameter Store

- **AWS System Manager - Parameter Store**

  - Stores external API URLs and configurations, e.g.,:
    - `/tlms/rwis/api_url`: RWIS endpoint
    - `/tlms/wzdx/api_url`: WZDx GeoJSON URL
    - `/tlms/incidents/api_url`: CHART incident XML URL
  - Lambda reads these parameters at runtime, so endpoints can be changed without code edits.

- **Amazon EventBridge (Scheduler)**

  - Triggers each Lambda poller on a regular schedule:
    - RWIS: every 15 minutes.
    - WZDx: hourly
    - Incidents: every 30 minutes
  - Each rule targets the corresponsing Lambda function with a simple JSON payload.

- **AWS Lambda (Pollers)**

  - **RWIS poller**:
    - Calls the RWIS API (ArcGIS/JSON).
    - Normalizes attributes (station ID, temperature, surface status, timestamps).
    - Sends one JSON line per observation into a Firehose delivery stream.
  - **WZDx pollers**:
    - Calls the WZDx GeoJSON endpoint.
    - Flattens `features[*].properties.core_details` and `geometry` into one record per `road_event_id`.
    - Sends JSON lines into an incidents Firehose delivery stream.

- **Amazon Kinesis Firehose**
  - Each feed has its own delivery stream (e.g., `rwis_firehose_to_s3_tlms`, `wzdx_firehose_to_s3_tlms`, `incidents_firehose_to_s3_tlms`).
  - Buffers records and writes **GZIP-compressed JSON** to the Landing Zone in S3.
  - Uses dedicated IAM roles to write to the correct bucket/prefix.

---

### 2.3 Transformation - AWS Glue (ETL Jobs & Crawlers)

- **Glue ETL Jobs (Landing -> Clean)**
  For each dataset, a PySpark job:

  - Reads raw JSON/GeoJSON/XML from the Landing Zone.
  - Applies an explicit schema and parsing logic.
  - Cleans and normalizes fields:
    - type conversions,
    - clamping invalid values,
    - standardizing enums (e.g., `surface_status` = `dry/wet/icy/unknown`).
  - Derives `event_time` and `dt` (date) fields.
  - Writes **Parquet** to the Clean Zone with partitioning.

- **Glue ETL Jobs (Clean -> Curated)**
  Second-stage jobs create denormalized and aggregated tables:

  - **RWIS:**
    - Build hourly per-station summaries (`rwis_hourly_summary`).
    - Build a "latest per station" snapshot (`rwis_latest_per_station`).
  - **WZDx:**
    - Build "current events" view (latest record per `road_event_id`).
    - Build daily summaries of work zones by date and direction.
  - **Incident/Events:**
    - Build "current incidents" view (latest record per `event_id`).
    - Build daily incident summaries by date, county, and direction.

- **AWS Glue Crawlers (Catalog)**
  - Crawlers run on clean and curated prefixes.
  - Update the **Glue Data Catalog** with Parquet tables for each dataset.
  - Tables are then queryable from Athena as `tlms_clean_db.*` and `tlms_curated_db.*`.

---

### 2.4 Query & Analytics - Athena and Tableau

- **Amazon Athena**

  - Connects to the Glue Data Catalog.
  - Queries clean and curated tables using standard SQL.
  - Used to implement analytical queries for the three project problems:
    - Multi-source daily coverage.
    - Weather vs incidents (temperature, icy/wet/dry vs incident counts).
    - Incident performance by county and alerts.

- **Tableau (or other BI tools)**
  - Athena query results are exported to CSV.
  - Visualization examples:
    - Daily multi-source coverage (RWIS vs WZDx vs Incidents).
    - Scatter plot of average temperature vs total incidents, with trend line.
    - Bar chart of incident volume by county, including alert incidents.

---

## 3. Per-Feed Pipeline Overview

### 3.1 RWIS Pipeline

1. **Ingest:**
   EventBridge -> RWIS Lambda -> Firehose -> `s3://tlms-landing-zone/rwis/raw/...`
2. **Landing -> Clean (Glue):**
   - Read raw JSON lines.
   - Apply schema (station_id, event_time, air_temp_f, pavement_temp_f, etc.).
   - Normalize surface status and temperature fields.
   - Write Parquet to `s3://tlms-clean-zone/rwis/validated/...` partitioned by `dt`, `station_id`.
3. **Clean -> Curated (Glue):**
   - Hourly summary per station (`rwis_hourly_summary`)
   - Latest per station snapshot (`rwis_latest_per_station`).
4. **Catalog & Query:**
   - Glue Crawler -> `tlms_clean_db.rwis_validated`, `tlms_curated_db.rwis_hourly_summary`, etc.
   - Athena queries + Tableau visualization.

### 3.2 WZDx Pipeline

1. **Ingest:**
   EventBridge -> WZDx Lambda -> Firehose -> `s3://tlms-landing-zone/wzdx/raw/...`
2. **Landing -> Clean (Glue):**
   - Read GeoJSON-derived JSON lines.
   - Flatten nested properties and geometry.
   - Normalize `road_event_id`, `direction`, `road_names`, and coordinates.
   - Write Parquet to `s3://tlms-clean-zone/wzdx/validated/...` partitioned by `dt`.
3. **Clean -> Curated (Glue):**
   - `wzdx_current_events`: latest row per `road_event_id`.
   - `wzdx_daily_summary`: daily work-zone counts and average durations by `dt`, and `direction`.
4. **Catalog & Query:**
   - Glue Crawler -> `tlms_clean_db.wzdx_validated`, `tlms_curated_db.wzdx_*`, etc.
   - Athena joins WZDx with RWIS and Incident/Events by date and direction.

---

### 3.3 Incident/Events Pipeline

1. **Ingest:**
   EventBridge -> Incident/Events Lambda -> Firehose -> `s3://tlms-landing-zone/incident-events/raw/...`
2. **Landing -> Clean (Glue):**
   - Parse XML `<Incident>` elements into flat records.
   - Extract `event_id`, `event_time`, `county`, `description`, `direction`, `lat`, `lon`, lane/alert fields.
   - Write Parquet to `s3://tlms-clean-zone/incident-events/validated/...` partitioned by `dt`.
3. **Clean -> Curated (Glue):**
   - `inc_current_events`: latest record per `event_id`.
   - `inc_daily_summary`: daily incident counts by `dt`, `county`, `direction`, and alert flag.
4. **Catalog & Query:**
   - Glue Crawler -> `tlms_clean_db.incident_validated`, `tlms_curated_db.inc_*`, etc.
   - Athena queries used for incident performance analysis by county.

---

## 4. Security and Configuration

- **IAM Roles** are defined for:
  - Lambda pollers (read Parameter Store, write to Firehose).
  - Firehose streams (write to s3).
  - Glue jobs and crawlers (read/write s3, update Data Catalog, write logs).
- **No credentials** are hardcoded in code. External URLs and parameters are stored in Parameter Store even though all datasets come from publicly available data.
- All data is stored in S3 with bucket policies restricting access to the appropriate service roles.

---

## Diagrams

Architecture and pipeline diagrams are stored in the [`diagrams/`](../diagrams/) folder, and can be referenced from the README and project documentation.

These diagrams visually summarize:

- The end-to-end TLMS architecture.
- Per-feed pipelines (RWIS, WZDx, Incidents)
- The landing -> clean -> curated flow and how it maps to AWS services.
