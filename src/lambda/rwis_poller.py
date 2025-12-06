"""
RWIS Poller - Lambda Function

This Lambda function retrieves Maryland RWIS (Road Weather Information System) observations from an external ArcGIS/JSON endpoint. The function: 

- Reads the RWIS API URL from AWS Systems Manager Parameter Store. 
- Calls the endpoint with appropriate headers and parses the JSON payload. 
- Normalizes station-level weather and pavement attributes (temperature, surface status, wind speed, precipitation type, timestamps).
- Optionally applies geofencing and record limits for controlled ingestion.
- Sends newline-delimited JSON records to a Kinesis Fireshose delivery stream, which writes GZIP-compressed raw data into the S3 Landing Zone. 

This function is triggered periodically via Amazon EventBridge to maintain near-real-time weather awareness for TLMS. 

"""

import os
import json
import math
import ssl
import time
import boto3
import urllib.request
import urllib.error

# --- config/env ---
FIREHOSE_STREAM = os.environ["FIREHOSE_STREAM"]
PARAM_API_URL = os.environ["PARAM_API_URL"]
PARAM_CENTER_LAT = os.environ["PARAM_CENTER_LAT"]
PARAM_CENTER_LON = os.environ["PARAM_CENTER_LON"]
PARAM_RADIUS_KM = os.environ["PARAM_RADIUS_KM"]
MAX_RECORDS = int(os.environ.get("MAX_RECORDS", "1000"))
SEND_RAW = os.environ.get("SEND_RAW", "false").lower() == "true"

ssm = boto3.client("ssm")
fh = boto3.client("firehose")


# -- helpers --
def get_param(name):
    return ssm.get_parameter(Name=name, WithDecryption=False)["Parameter"]["Value"]


def _to_float(v):
    try:
        return None if v in (None, "", "null") else float(v)
    except Exception:
        return None


def _to_ms(ts_like):
    """
    Accept epoch ms / sec or ISO8601 'YYYY-MM-DDTHH:MM:SS[Z]' and return epoch ms.
    """
    if isinstance(ts_like, (int, float)):
        v = int(ts_like)
        if v > 10**12:     # already ms
            return v
        if v > 10**10:     # ms
            return v
        if v > 10**6:      # seconds
            return v * 1000
    if isinstance(ts_like, str):
        s = ts_like.strip().rstrip("Z")[:19]  # 'YYYY-MM-DDTHH:MM:SS'
        try:
            return int(time.mktime(time.strptime(s, "%Y-%m-%dT%H:%M:%S"))) * 1000
        except Exception:
            return None
    return None

# Function converting from F to C which we are reverting to keep things clean for Maryland use cases. We are commenting this out since we are not
# going to use it but, if use case changes and temperatures need to be converted, we can re-activate this function.
# def _f_to_c(f):
#    return None if f is None else (f - 32.0) * 5.0 / 9.0

# Function converting from mph to kmh which we are reverting to keep things clean for Maryland use cases. We are commenting this out since we are not
# going to use it but, if use case changes and temperatures need to be converted, we can re-activate this function.
# def _mph_to_kph(m):
#   return None if m is None else m * 1.60934


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * \
        math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return 2 * R * math.asin(math.sqrt(a))


def _fetch_json(url, headers=None, timeout=20):
    ctx = ssl.create_default_context()
    req = urllib.request.Request(
        url,
        headers=(headers or {"Accept": "application/json",
                 "User-Agent": "TLMS-RWIS-Ingest/1.0"})
    )
    with urllib.request.urlopen(req, context=ctx, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


# -- Normalizer, tuned to the RWIS records received in the last API test, identifyin proper naming in the API and making sure that we are normalizing the data accordingly --
def normalize_record(rec):
    """
    rec looks like:
      {
        "rowid": 1,
        "collector_id": 1,
        "station_id": 2,
        "ReportTime": 1763359140000,
        "AirTemperature": 42,
        "MaxSurfaceTemperature": 45,
        "MinSurfaceTemperature": 32,
        "PrecipiationType": "None",
        "AvgWindSpeed": 3,
        "WindGust": 7,
        "LATITUDE": 39.27,
        "LONGITUDE": -77.32,
        "_geom": {"x": -77.32, "y": 39.27},
        ...
      }
    """
    a = rec

    # -- station id --
    station_id = a.get("station_id") or a.get(
        "StationID") or a.get("stationId") or -1

    # -- timestamp from ReportTime (ms) --
    ts_ms = _to_ms(a.get("ReportTime")) or int(time.time() * 1000)

    # -- temperatures (°F -> °C) -- Re-enable this in case the use case changes and temperatures need to be converted.
    # air_f  = _to_float(a.get("AirTemperature"))
    # air_c  = _f_to_c(air_f) if air_f is not None else None
    # pave_f = _to_float(a.get("MaxSurfaceTemperature") or a.get("MinSurfaceTemperature"))
    # pave_c = _f_to_c(pave_f) if pave_f is not None else None

    # -- Temperatures in Fahrenheit --
    air_temp_f = _to_float(a.get("AirTemperature"))
    pave_temp_f_main = _to_float(a.get("MaxSurfaceTemperature"))
    pave_temp_f_alt = _to_float(a.get("MinSurfaceTemperature"))
    pavement_temp_f = pave_temp_f_main if pave_temp_f_main is not None else pave_temp_f_alt

    # -- wind (mph -> kph) --
    wind_mph = _to_float(a.get("AvgWindSpeed") or a.get("WindGust"))
    # wind_kph = _mph_to_kph(wind_mph) if wind_mph is not None else None -- use this in case wind speeds need to be converted to kph.

    # -- precip & surface condition --
    precip = a.get("PrecipiationType") or a.get(
        "PrecipitationType") or a.get("precipType")

    # simple surface_status rules using precip & pavement_temp_f
    surface_status = "unknown"
    p = (precip or "").lower()
    if p in ("", "none", "no precip", "no precipitation"):
        surface_status = "dry"
    else:
        if pavement_temp_f is not None and pavement_temp_f <= 32.0:
            surface_status = "icy"
        else:
            surface_status = "wet"

    # -- geometry (for geofence only) --
    lat = _to_float(a.get("LATITUDE"))
    lon = _to_float(a.get("LONGITUDE"))
    if lat is None or lon is None:
        g = a.get("_geom") or {}
        lon = lon or _to_float(g.get("x"))
        lat = lat or _to_float(g.get("y"))

    return {
        "station_id": int(station_id) if str(station_id).isdigit() else -1,
        "ts": ts_ms,
        "air_temp_f": air_temp_f,
        "pavement_temp_f": pavement_temp_f,
        "precip_type": precip or "None",
        "wind_mph": wind_mph,
        "surface_status": surface_status,
        "lat": lat,
        "lon": lon,
    }


# -- handler --
def lambda_handler(event, context):
    # Params
    api_url = get_param(PARAM_API_URL)
    center_lat = float(get_param(PARAM_CENTER_LAT))
    center_lon = float(get_param(PARAM_CENTER_LON))
    radius_km = float(get_param(PARAM_RADIUS_KM))

    # Fetch once
    payload = _fetch_json(api_url)
    print("DEBUG_PAYLOAD_TYPE:", type(payload).__name__)
    if isinstance(payload, dict):
        print("DEBUG_TOPLEVEL_KEYS:", list(payload.keys())[:12])

    # If someone accidentally gives the MapServer root, hop to /0/query - Added this because I was pointing to the wrong API
    # I was getting empty responses, while debugging I figured it out but will leave this code just in case.
    if isinstance(payload, dict) and "layers" in payload and "features" not in payload:
        base = api_url.rstrip("/")
        if "/MapServer" in base:
            base = base.split("/MapServer")[0] + "/MapServer"
        query_url = f"{base}/0/query?where=1%3D1&outFields=*&f=json&resultRecordCount={MAX_RECORDS}"
        print("DEBUG_REFETCH_QUERY_URL:", query_url)
        payload = _fetch_json(query_url)

    # Extract items: ArcGIS FeatureSet -> attributes + geometry
    items = []
    if isinstance(payload, dict) and isinstance(payload.get("features"), list):
        items = [
            (f.get("attributes") or {}) | {"_geom": (f.get("geometry") or {})}
            for f in payload["features"]
        ]
        print("DEBUG_ITEMS_KEY: features LEN:", len(items))
    elif isinstance(payload, list):
        items = payload

    try:
        print("DEBUG_RAW_SAMPLE:", json.dumps(items[:2], default=str)[:800])
    except Exception:
        pass

    # Normalize + geofence
    norm = []
    for rec in items:
        n = normalize_record(rec)
        # geofence (if we have coords)
        if radius_km > 0 and n.get("lat") is not None and n.get("lon") is not None:
            d = haversine_km(center_lat, center_lon, n["lat"], n["lon"])
            if d > radius_km:
                continue
        # drop lat/lon before sending
        norm.append({k: v for k, v in n.items() if k not in ("lat", "lon")})
        if len(norm) >= MAX_RECORDS:
            break

    print("DEBUG_SAMPLE_NORMALIZED:", json.dumps(norm[:3], default=str))

    # Choose payload to send
    to_send = items if SEND_RAW else norm

    # Firehose batch (<= 500)
    sent = 0
    batch = []
    for rec in to_send:
        batch.append({"Data": (json.dumps(rec) + "\n").encode("utf-8")})
        if len(batch) == 500:
            resp = fh.put_record_batch(
                DeliveryStreamName=FIREHOSE_STREAM, Records=batch)
            sent += (len(batch) - resp.get("FailedPutCount", 0))
            batch = []
    if batch:
        resp = fh.put_record_batch(
            DeliveryStreamName=FIREHOSE_STREAM, Records=batch)
        sent += (len(batch) - resp.get("FailedPutCount", 0))

    print("DEBUG_SENT_COUNT:", sent, "MODE:",
          "RAW" if SEND_RAW else "NORMALIZED")
    return {"attempted": len(items), "sent": sent, "mode": "raw" if SEND_RAW else "normalized"}
