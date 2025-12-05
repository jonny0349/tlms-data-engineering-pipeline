## Problem Statement - Traffic Lane Management System (TLMS)

This project addresses the challenge of integrating multiple transportation data sources for the State of Maryland into a single, analytics-ready data platform. Currently, different systems publish valuable information about **weather conditions, work zones, and traffic incidents**, but they are siloed, inconsistent, and difficult to analyze together.

The TLMS data engineering pipeline was designed to solve three main problems:

---

## 1. Data from many sources does not line up

Maryland transportation data comes from several independent feeds:

- **RWIS (Road Weather Information System):** road weather and pavement conditions from sensor stations.
- **WZDx (Work Zone Data Exchange):** standardized work-zone information in GeoJSON format.
- **CHART Incident/Events:** crash reports, disabled vehicles, lane closures, and related events.

Each feed:

- Uses **different formats** (JSON, GeoJSON, XML, GZIP)
- Uses **different field names and schemas**.
- Updates on **different schedules**.
- Is not stored together in a queryable, historical store.

As a result:

- It is difficult to answer basic cross-cutting questions like: _"On days with major work zones and bad weather, do incidents increase?_
- Analysis must manually download, clean, and align data from multiple systems, which is slow and error-prone.

**Goal:** Build a unified, scheduled ingestion and transformation pipeline so all three sources land in a consistent, joinable structure in an AWS data lake.

---

## 2. Slow, siloed incident and weather awareness

Operations and planning teams need to understand how **weather** and **work zones** relate to **incidents**, especially during active events:

- Are certain corridors more vulnerable during freezing temperatures?
- Do specific combinations of **work-zone activity + weather** correlate with higher incident counts?
- Can we compare "normal" vs "storm" or "work-zone-heavy" days?

**Goal:** Create curated tables (e.g., RWIS hourly summaries, incident daily summaries, WZDx daily summaries) that make it easy to:

- Relate **weather conditions** (temperature, icy/wet/dry indicators) to
- **Incident activity** (counts, alerts) and
- **Work-zone presence** (active events by day and direction)

These curated tables are then queried in Athena and visualized in Tableau to provide much faster situational awareness.

---

## 3. Limited ability to measure performance with current reporting

Existing reporting make it hard to answer performance questions such as:

- Which **counties** experience the highest incident load on a typical day?
- How many of those incidents are **alert-level** events?
- Are there corridors or regions that consistently require more attention or resources?
- How good is the underlying data quality (for example, are closure statuses being updated)?

Problems today:

- Incident data is not stored in a historical, analytics-friendly format.
- Some fields (such as incident closure status) are incomplete or unreliable, making traditional KPIs like "% closed incidents" difficult to compute.
- There is no single, curated table that summarizes **incident volume and severity by county and date**.

**Goal:** Build a curated **incident performance** layer that:

- Aggregates incident volume at the **daily x county x direction** level.
- Captures **alert incidents** explicitly.
- Exposes data quality issues (e.g., closure flags that are always "open") instead of hiding them.

This enables:

- Comparisons of incident load across counties.
- Identification of hotspots that may need more resources of infrastructure changes.
- A more honest view of data quality, which is itself a performance insight.

---

## How the TLMS pipeline addresses these problems

Across all three problems, the TLMS data engineering pipeline:

- **Centralizes** data from RWIS, WZDx, and Incident/Events in S3 using a Landing -> Clean -> Curated zone design.
- **Standardizes** schemas and timestamps so the datasets can be joined on common keys (e.g., date, location).
- **Denormalizes and aggregates** into curated tables that are easy to query with Athena.
- **Enable BI tools** (e.g., Tableau) to visualize multi-source coverage, weather-incident relationships, and county-level performance.

The rest of this repository documents the architecture, transformation logic, and SQL used to implement this pipeline on AWS.
