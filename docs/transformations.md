# Data Transformations Sumary

This document summarizes all major transformation steps appplied to the three TLMS datasets: **RWIS**, **WZDx**, **Incident/Events**, as they move from the Landing zone -> Clean zone -> Curated zone.

Each transformation category aligns with typical data engineering practices: cleansing, normalization, enrichment, denormalization, aggregation, and partitioning.

---

# Overview Table of Transformations

| Transformation Type                 | RWIS (Weather Sensors)                                                                                                                                                                   | WZDx (Work Zones)                                                                                                                                 | Incidents (Traffic Events)                                                                                                          |
| ----------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| **Data Cleansing**                  | • Remove null/invalid temps<br>• Clamp enums (surface_status = dry/wet/icy/unknown)<br>• Cast timestamps to `event_time` and derive `dt`<br>• Ensure numeric types for temps, wind, etc. | • Handle missing direction, road names, geometry<br>• Normalize coordinate extraction<br>• Cast timestamps from string → timestamp → date         | • Parse XML into flat fields<br>• Normalize missing county, direction, lat/lon<br>• Clean empty strings, standardize boolean fields |
| **Normalization / Standardization** | • Convert temps to Fahrenheit<br>• Standardize surface_status categories<br>• Normalize station_id and numeric fields                                                                    | • Normalize `direction` → uppercase (N/S/E/W variants)<br>• Standardize road_event_id fallback logic<br>• Convert start/end times into timestamps | • Normalize incident_type, direction<br>• Standardize event_time and dt formats<br>• Harmonize alert flags                          |
| **Enrichment**                      | • Add `event_time` (timestamp) from millis<br>• Add partition key `dt` (yyyy-MM-dd)<br>• Derive `event_hour` for hourly grouping                                                         | • Extract begin/end coordinates from GeoJSON geometry<br>• Derive `dt` for time partitioning                                                      | • Convert timestamps from XML<br>• Derive `dt` for partitioning<br>• Extract lane impact summary                                    |
| **Denormalization**                 | **Hourly Summary:** one wide record per (station_id, hour)<br>**Latest Per Station:** single “current condition” row per station                                                         | **Current Events:** one wide row per `road_event_id` (latest record)<br>Flatten properties.core_details + geometry into final schema              | **Current Incidents:** one wide row per `event_id` (latest record)<br>Flatten XML fields into final schema                          |
| **Aggregation**                     | • Average temps per hour<br>• Count dry/wet/icy occurrences<br>• Observation counts                                                                                                      | • Daily work-zone counts<br>• Avg duration_hours<br>• Earliest start / latest end                                                                 | • Daily incident counts by dt × county × direction<br>• Alert incidents<br>• Latest occurrence time                                 |
| **Partitioning Strategy**           | `dt` and `station_id` partitions (Parquet)                                                                                                                                               | `dt` partitions (validated) and `snapshot_dt` (curated)                                                                                           | `dt` partitions (validated) and `snapshot_dt` (curated)                                                                             |

---

# Detailed Notes by Dataset

## 1. RWIS Transformations

- **RAW -> Clean (Landing -> Clean Glue Job)**

  - Apply explicit schema
  - Normalize temperatures: convert F where needed (commented this out since the use case didn't require temperature convertions).
  - Clamp surface_status values
  - Convert ms timestamps -> timestamp -> dt
  - Output partitioned Parquet to `rwis/validated/`

- **Clean -> Curated (Hourly -> Latest Snapshots)**
  - Hourly summary (`rwis_hourly_summary`):
    - avg temps, avg wind, icy/wet/dry counts, obs_count
  - Latest per station (`rwis/latest_per_station/`):
    - Window function picks the most recent record per station
    - Produces denormalized "current conditions" tables

---

## 2. WZDx Transformations

- **Raw -> Clean**

  - Flatten GeoJSON feature structure
  - Extract road_event_id, direction, road_names
  - Extract being/end lat/lon from geometry
  - Convert date strings -> timestamps
  - Derive dt
  - Write Parquet to `wzdx/validated`

- **Clean -> Curated**
  - **Current Events**
    - Window function: pick latest record per `road_event_id`
    - Produces denormalized wide row per event
  - **Daily summary**
    - Count work zones per dt x direction
    - Compute average duration in hours

---

## 3. Incident Transformations

- **Raw -> Clean**

  - Parse XML `<Incident>` elements into JSON
  - Flatten nested fields (location, lanes, alert flags)
  - Normalize timestamps, direction, county
  - Derive dt
  - Write Parquet to `incident-events/validated/`

- **Clean -> Curated**
  - **Current Incidents:**
    - Latest record per `event_id` (window function)
    - Denormalized snapshot for operational use
  - **Daily summary:**
    - Incident counts by dt x county x direction
    - Alert incident counts
    - Latest occurrence time

---

## Why this Matters

These transformations solve the project's original data challenges:

1. **Cross-source alignment** - all datasets on a common `dt`
2. **Historical analysis** through partitioned Parquet.
3. **Operational awareness** via curated "current events" snapshots.
4. **Weather-work-zone-incident correlation** through joinable curated tables.

The curated zone is now fully analytics-ready and supports Athena queries for analysis in different BI tools.

---
