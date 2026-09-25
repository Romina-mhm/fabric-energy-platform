# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "82f9a133-f37f-4546-9ebb-0ebf0a7c6fb8",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "a9b50eeb-8564-4e09-ac18-87c3583d27a3",
# META       "known_lakehouses": [
# META         {
# META           "id": "82f9a133-f37f-4546-9ebb-0ebf0a7c6fb8"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

mode = "incremental"                # "test" | "backfill" | "incremental"
backfill_start = "2024-01-01"
test_days = 3
archive_delay_days = 6       # Open-Meteo reanalysis archive lags ~5 days
archive_repull_days = 7      # incremental: re-download the last N archive days (reanalysis gets revised)
forecast_past_days = 7       # recent days from the forecast model, until the archive catches up
forecast_days = 2

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json, time, uuid
from datetime import datetime, timezone
import pandas as pd
import requests

spark.conf.set("spark.sql.session.timeZone", "UTC")

HOURLY_VARS = "temperature_2m,wind_speed_10m,wind_speed_100m,shortwave_radiation,cloud_cover,precipitation"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:6]
today_utc = pd.Timestamp.now(tz="UTC").normalize()
print("run_id:", RUN_ID, "| mode:", mode)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

locations = spark.sql("""
    SELECT w.location_code, w.country_code, w.latitude, w.longitude
    FROM weather_location w
    JOIN country c ON c.country_code = w.country_code
    WHERE c.is_active
    ORDER BY w.location_code
""").collect()
print(len(locations), "locations:", [r.location_code for r in locations])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

locations = spark.sql("""
    SELECT w.location_code, w.country_code, w.latitude, w.longitude
    FROM weather_location w
    JOIN country c ON c.country_code = w.country_code
    WHERE c.is_active
    ORDER BY w.location_code
""").collect()
print(len(locations), "locations:", [r.location_code for r in locations])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

archive_end = today_utc - pd.Timedelta(days=archive_delay_days)

watermarks = {}
if mode == "incremental":
    for r in spark.sql("""SELECT area_key, loaded_until_utc FROM ctl_watermark
                          WHERE source = 'open_meteo' AND dataset = 'weather_hourly_archive'""").collect():
        watermarks[r.area_key] = pd.Timestamp(r.loaded_until_utc).tz_localize("UTC")

def archive_start(loc):
    if mode == "test":
        return archive_end - pd.Timedelta(days=test_days)
    if mode == "incremental" and loc in watermarks:
        return watermarks[loc] - pd.Timedelta(days=archive_repull_days)
    return pd.Timestamp(backfill_start, tz="UTC")

def year_chunks(start, end):
    chunks, cur = [], start
    while cur < end:
        nxt = min(pd.Timestamp(year=cur.year + 1, month=1, day=1, tz="UTC"), end)
        chunks.append((cur, nxt))
        cur = nxt
    return chunks

jobs = []
for loc in locations:
    for s, e in year_chunks(archive_start(loc.location_code), archive_end):
        jobs.append(("weather_hourly_archive", loc, s, e))
    jobs.append(("weather_hourly_forecast", loc,
                 today_utc - pd.Timedelta(days=forecast_past_days),
                 today_utc + pd.Timedelta(days=forecast_days)))
print(len(jobs), "API calls planned")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_json(url, params, attempts=4):
    """GET with generous timeout and retries on timeouts, connection errors and 429/5xx."""
    for i in range(attempts):
        try:
            r = requests.get(url, params=params, timeout=(10, 240))   # 10 s to connect, 240 s to read
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            if i < attempts - 1:
                time.sleep(20 * (i + 1))        # 20 s, 40 s, 60 s back-off
                continue
            raise
        if r.status_code in (429, 500, 502, 503, 504) and i < attempts - 1:
            time.sleep(30 * (i + 1))
            continue
        r.raise_for_status()
        return r.json()

def fetch_weather(job):
    dataset, loc, start, end = job
    base = dict(latitude=float(loc.latitude), longitude=float(loc.longitude),
                hourly=HOURLY_VARS, timezone="UTC")
    if dataset == "weather_hourly_archive":
        url = ARCHIVE_URL
        # end_date is inclusive in Open-Meteo, our windows are end-exclusive
        params = {**base, "start_date": f"{start:%Y-%m-%d}",
                  "end_date": f"{(end - pd.Timedelta(days=1)):%Y-%m-%d}"}
    else:
        url = FORECAST_URL
        params = {**base, "past_days": forecast_past_days, "forecast_days": forecast_days}

    kind = dataset.replace("weather_hourly_", "")
    path = (f"Files/raw/weather/{kind}/{loc.location_code}/{start:%Y}/"
            f"weather_{kind}__{loc.location_code}__{start:%Y%m%d}_{end:%Y%m%d}__{RUN_ID}.json")
    rec = dict(run_id=RUN_ID, source="open_meteo", dataset=dataset, area_key=loc.location_code,
               request_start_utc=start.to_pydatetime(), request_end_utc=end.to_pydatetime(),
               status=None, file_path=None, bytes=None, points=None, error_message=None)
    try:
        payload = get_json(url, params)
        text = json.dumps(payload)
        notebookutils.fs.put(path, text, True)
        rec.update(status="ok", file_path=path, bytes=len(text.encode("utf-8")),
                   points=len(payload.get("hourly", {}).get("time", [])))
    except Exception as e:
        rec.update(status="error", error_message=f"{type(e).__name__}: {str(e)[:500]}")
    rec["ingested_at_utc"] = datetime.now(timezone.utc)
    return rec

LOG_SCHEMA = spark.table("ctl_ingest_log").schema
LOG_COLS = [f.name for f in LOG_SCHEMA]

def write_log(records):
    if records:
        rows = [tuple(r.get(c) for c in LOG_COLS) for r in records]
        spark.createDataFrame(rows, LOG_SCHEMA).write.mode("append").saveAsTable("ctl_ingest_log")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = []
for i, job in enumerate(jobs, 1):
    results.append(fetch_weather(job))
    time.sleep(5)                       # be polite to the free API
    if i % 10 == 0:
        print(f"{i}/{len(jobs)} calls done")
write_log(results)
print("Finished:", len(results), "calls")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if mode == "test":
    print("Test mode: watermark not updated")
else:
    res = pd.DataFrame(results)
    arch = res[res.dataset == "weather_hourly_archive"]
    summary = (arch.assign(is_error=arch.status.eq("error"))
                   .groupby("area_key")
                   .agg(errors=("is_error", "sum"), max_end=("request_end_utc", "max"))
                   .reset_index())
    ok = summary[summary.errors == 0]
    wm_rows = [(r.area_key, pd.Timestamp(r.max_end).to_pydatetime()) for r in ok.itertuples()]
    if wm_rows:
        spark.createDataFrame(wm_rows, "area_key string, loaded_until_utc timestamp") \
             .createOrReplaceTempView("wm_updates")
        spark.sql("""
            MERGE INTO ctl_watermark t
            USING (SELECT 'open_meteo' AS source, 'weather_hourly_archive' AS dataset, area_key,
                          loaded_until_utc, current_timestamp() AS updated_at_utc
                   FROM wm_updates) s
            ON t.source = s.source AND t.dataset = s.dataset AND t.area_key = s.area_key
            WHEN MATCHED THEN UPDATE SET t.loaded_until_utc = s.loaded_until_utc,
                                         t.updated_at_utc  = s.updated_at_utc
            WHEN NOT MATCHED THEN INSERT *
        """)
    print(f"Watermark advanced for {len(wm_rows)} locations")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

res = pd.DataFrame(results)
display(res.groupby(["dataset", "status"]).size().unstack(fill_value=0))
display(res[res.status == "error"][["dataset", "area_key", "error_message"]])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.session.stop()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
