"""
Lambda poller for WZDx (Work Zone Data Exchange) feed. 

- Reads the WZDx GeoJSON endpoint URL from SSM Parameter Store. 
- Fetches the latest FeatureCollection.
- Flattens nested GeoJSON features (properties.core_details + geometry) into one JSON record per work zone (road_event_id)
- Sends newline-delimited JSON records into a Kinesis Firehose delivery stream, which writes into the S3 landing zone
"""


import os
import json
import ssl
import time
import urllib.request
import urllib.error

import boto3

# --- config/env ---
FIREHOSE_STREAM = os.environ["FIREHOSE_STREAM"]
PARAM_API_URL = os.environ["PARAM_API_URL"]
MAX_FEATURES = int(os.environ.get("MAX_FEATURES", "500"))

ssm = boto3.client("ssm")
fh = boto3.client("firehose")


def get_param(name: str) -> str:
    # Read a plain-text parameter from SSM.
    return ssm.get_parameter(Name=name, WithDecryption=False)["Parameter"]["Value"]


def lambda_handler(event, context):
    # 1) Resolve API URL from Parameter Store
    api_url = get_param(PARAM_API_URL)

    # 2) Call WZDx GeoJSON endpoint
    headers = {
        "User-Agent": "TLMS-WZDx-Ingest/1.0",
        "Accept": "application/json",
    }
    ctx = ssl.create_default_context()
    req = urllib.request.Request(api_url, headers=headers, method="GET")

    try:
        with urllib.request.urlopen(req, context=ctx, timeout=20) as r:
            raw = r.read()
            payload = json.loads(raw.decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"HTTPError {e.code} on {api_url}: {e.reason}")
    except Exception as e:
        # If JSON parse fails, log first 400 chars for debugging
        snippet = raw[:400].decode(
            "utf-8", errors="replace") if "raw" in locals() else "<no body>"
        raise RuntimeError(
            f"WZDx did not return valid JSON. First 400 chars: {snippet}") from e

    # 3) Validate that this is a FeatureCollection with a 'features' list
    if not (isinstance(payload, dict) and isinstance(payload.get("features"), list)):
        raise RuntimeError(
            f"Unexpected WZDx payload structure. "
            f"type={type(payload).__name__}, keys={list(payload.keys()) if isinstance(payload, dict) else 'n/a'}"
        )

    features = payload["features"][:MAX_FEATURES]
    print("DEBUG_FEATURES_COUNT:", len(features))

    # log a sample of properties keys for debugging / mapping validation since we are getting road_event_id and direction as nulls and they are
    # showing empty in Athena, we're guessing the mapping wrong
    if features:
        sample_props = (features[0].get("properties") or {})
        # We are not getting direction or road_event_id at the top level properties, have to try to get them from core_details
        core_details = (sample_props.get("core_details") or {})
        print("DEBUG_FEATURES_SAMPLE_PROPS_KEYS:", list(sample_props.keys()))
        print("DEBUG_CORE_DETAILS_KEYS:", list(core_details.keys()))

    # 4) Build records for Firehose: one JSON line per feature
    #    We lightly flatten some common properties to align later with RWIS joins:
    #    - road_event_id
    #    - road_names
    #    - direction
    #    - start_date / end_date
    #    - begin / end lat/lon (from geometry if available)
    sent = 0
    batch = []
    now_ms = int(time.time() * 1000)

    for f in features:
        props = f.get("properties", {}) or {}
        geom = f.get("geometry", {}) or {}
        coords = geom.get("coordinates")
        core = props.get("core_details", {}) or {}

        # road_event_id from core_details (primary) then fallbacks
        road_event_id = (
            core.get("road_event_id")
            or core.get("id")
            or props.get("road_event_id")
            or props.get("id")
            or f.get("id")
        )

        # road_names, typically in core_details.road_names
        road_names = core.get("road_names") or props.get("road_names")

        # direction from core_details, then props
        direction = (
            core.get("direction")
            or props.get("direction")
            or core.get("directionality")
            or props.get("directionality")
        )
        if isinstance(direction, str):
            direction = direction.upper()

        # Very lightweight geometry begin/end coordinate extraction for LineString / MultiLineString
        begin_lat = begin_lon = end_lat = end_lon = None
        try:
            # WZDx uses GeoJSON, typically LineString or MultiLineString
            if geom.get("type") == "LineString" and coords:
                # coords: [[lon, lat], [lon, lat], ...]
                begin_lon, begin_lat = coords[0]
                end_lon,   end_lat = coords[-1]
            elif geom.get("type") == "MultiLineString" and coords:
                first_line = coords[0]
                last_line = coords[-1]
                if first_line:
                    begin_lon, begin_lat = first_line[0]
                if last_line:
                    end_lon, end_lat = last_line[-1]
        except Exception:
            # If geometry is weird, just leave coords as None
            pass

        record = {
            "ingest_ts": now_ms,
            "road_event_id": road_event_id,
            "road_names": road_names,
            "direction": direction,
            "start_date": props.get("start_date"),
            "end_date": props.get("end_date"),
            "begin_lat": begin_lat,
            "begin_lon": begin_lon,
            "end_lat": end_lat,
            "end_lon": end_lon,
            # "raw_feature": f,  # keep full feature for future enrichment/geo, commenting this out because it looks like storing raw features is too large
            # and glue is getting confused with "direction" since we're getting an empty string even though "direction" is being passed inside the core_features.
        }

        line = json.dumps(record) + "\n"
        batch.append({"Data": line.encode("utf-8")})

        if len(batch) == 500:
            resp = fh.put_record_batch(
                DeliveryStreamName=FIREHOSE_STREAM,
                Records=batch,
            )
            sent += (len(batch) - resp.get("FailedPutCount", 0))
            batch = []

    # Flush remaining batch
    if batch:
        resp = fh.put_record_batch(
            DeliveryStreamName=FIREHOSE_STREAM,
            Records=batch,
        )
        sent += (len(batch) - resp.get("FailedPutCount", 0))

    print("DEBUG_SENT_COUNT:", sent)
    return {"attempted": len(features), "sent": sent}
