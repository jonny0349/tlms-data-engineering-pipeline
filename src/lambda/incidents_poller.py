"""
Incidents Poller - Lambda Function

This Lambda function ingests CHART traffic incident/events from an XML endpoint.
The function: 

- Reads the incident XML API URL from AWS Systems Manager Parameter Store. 
- Downloads and parses the XML <Incident> list into flattened JSON objects. 
- Extracts key attributes including event_id, event_time, county, direction, incident_type, location, lane status, and traffic alert flags. 
- Normalizes timestamps and coordinates. 
- Emits one JSON line per incident record to a Kinesis Firehose delivery stream, which stores raw GZIP data in the S3 Landing zone. 

Triggered by EventBridge (e.g., every 30 minutes), this poller maintains a continuous historical record of Maryland traffic incidents. 
"""

import os
import json
import time
import ssl
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET

import boto3

# Environment Configuration
FIREHOSE_STREAM = os.environ["FIREHOSE_STREAM"]
PARAM_API_URL = os.environ["PARAM_API_URL"]
MAX_RECORDS = int(os.environ.get("MAX_RECORDS", "1000"))

ssm = boto3.client("ssm")
fh = boto3.client("firehose")


def get_param(name: str) -> str:
    return ssm.get_parameter(Name=name, WithDecryption=False)["Parameter"]["Value"]


def _get_text(elem: ET.Element, tag: str):
    # Safely get the text of a child element, or None.
    if elem is None:
        return None
    child = elem.find(tag)
    return child.text if child is not None else None


def _to_float(v):
    try:
        return float(v) if v not in (None, "", "null") else None
    except Exception:
        return None


def normalize_incident(incident_elem: ET.Element, ingest_ts_ms: int) -> dict:
    # Map one <Incident> XML element into our normalized incident schema
    # aligned with RWIS/WZDx concepts.
    # These tag names are based on CHART Incident XML feed docs.

    event_id = _get_text(incident_elem, "id")
    create_time = _get_text(incident_elem, "createTime")
    start_time = _get_text(incident_elem, "startDateTime")
    closed_text = _get_text(incident_elem, "closed")
    county = _get_text(incident_elem, "county")
    description = _get_text(incident_elem, "description")
    direction = _get_text(incident_elem, "direction")
    incident_type = _get_text(incident_elem, "incidentType")
    lat_str = _get_text(incident_elem, "latitude") or _get_text(
        incident_elem, "lat")
    lon_str = _get_text(incident_elem, "longitude") or _get_text(
        incident_elem, "lon")
    lanes_status = _get_text(incident_elem, "lanesStatus")
    traffic_alert = _get_text(incident_elem, "trafficAlert")
    alert_msg = _get_text(incident_elem, "trafficAlertTextMsg")

    # Normalize direction to uppercase (align with RWIS/WZDx)
    if isinstance(direction, str):
        direction = direction.upper()

    # Closed as boolean if possible
    closed = None
    if isinstance(closed_text, str):
        closed_lower = closed_text.strip().lower()
        if closed_lower in ("true", "t", "1", "yes", "y"):
            closed = True
        elif closed_lower in ("false", "f", "0", "no", "n"):
            closed = False

    # traffic_alert as boolean if possible
    traffic_alert_bool = None
    if isinstance(traffic_alert, str):
        al = traffic_alert.strip().lower()
        if al in ("true", "t", "1", "yes", "y"):
            traffic_alert_bool = True
        elif al in ("false", "f", "0", "no", "n"):
            traffic_alert_bool = False

    # Lat/lon numeric
    lat = _to_float(lat_str)
    lon = _to_float(lon_str)

    return {
        "ingest_ts": ingest_ts_ms,
        "event_id": event_id,
        "create_time": create_time,   # ISO text; parsed later in Glue
        "start_time": start_time,     # ISO text; parsed later in Glue
        "closed": closed,
        "county": county,
        "description": description,
        "direction": direction,
        "incident_type": incident_type,
        "lat": lat,
        "lon": lon,
        "lanes_status": lanes_status,
        "traffic_alert": traffic_alert_bool,
        "traffic_alert_msg": alert_msg,
    }


def lambda_handler(event, context):
    # Get API URL from Parameter Store
    api_url = get_param(PARAM_API_URL)

    # Fetch XML from CHART incidents feed
    headers = {
        "User-Agent": "TLMS-Incidents-Ingest/1.0",
        "Accept": "application/xml",
    }
    ctx = ssl.create_default_context()
    req = urllib.request.Request(api_url, headers=headers, method="GET")

    try:
        with urllib.request.urlopen(req, context=ctx, timeout=20) as r:
            raw = r.read()
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"HTTPError {e.code} on {api_url}: {e.reason}")
    except Exception as e:
        raise RuntimeError(f"Error fetching incidents feed: {e}") from e

    # Parse XML
    try:
        root = ET.fromstring(raw)
    except Exception as e:
        snippet = raw[:400].decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Incidents XML parse failed. First 400 chars: {snippet}") from e

    # Find all <Incident> elements (tag names are case-insensitive-ish)
    incidents = []
    for elem in root.iter():
        tag_lower = elem.tag.lower()
        if tag_lower.endswith("incident"):  # e.g., 'Incident' or 'incident'
            # Avoid counting root tags if they are <Incidents>
            if tag_lower == "incident" or tag_lower.endswith("incident"):
                # Heuristic: treat only leaf-level Incident nodes
                if list(elem):  # has children
                    incidents.append(elem)

    print("DEBUG_INCIDENT_COUNT:", len(incidents))

    if not incidents:
        # maybe the tag name is different; log root tag
        print("DEBUG_ROOT_TAG:", root.tag)

    # Normalize and send to Firehose
    now_ms = int(time.time() * 1000)
    batch = []
    sent = 0

    for inc in incidents[:MAX_RECORDS]:
        norm = normalize_incident(inc, now_ms)
        line = json.dumps(norm) + "\n"
        batch.append({"Data": line.encode("utf-8")})

        if len(batch) == 500:
            resp = fh.put_record_batch(
                DeliveryStreamName=FIREHOSE_STREAM,
                Records=batch,
            )
            sent += (len(batch) - resp.get("FailedPutCount", 0))
            batch = []

    if batch:
        resp = fh.put_record_batch(
            DeliveryStreamName=FIREHOSE_STREAM,
            Records=batch,
        )
        sent += (len(batch) - resp.get("FailedPutCount", 0))

    print("DEBUG_SENT_COUNT:", sent)
    return {"attempted": min(MAX_RECORDS, len(incidents)), "sent": sent}
