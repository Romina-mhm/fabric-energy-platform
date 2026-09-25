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

mode = "incremental"            # "test" (3 days, no watermark update) | "backfill" | "incremental"
test_days = 3
backfill_start = "2024-01-01"
repull_days = 3          # incremental: re-download the last N days to catch ENTSO-E corrections
datasets = "actual_load,actual_generation,day_ahead_price,crossborder_flow"
max_workers = 4          # parallel API calls (ENTSO-E allows ~400 requests/minute)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import uuid
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from entsoe import EntsoeRawClient
from entsoe.exceptions import NoMatchingDataError

spark.conf.set("spark.sql.session.timeZone", "UTC")

KEY_VAULT_URI = "https://energyanalytics-kv.vault.azure.net/"
api_key = notebookutils.credentials.getSecret(KEY_VAULT_URI, "entsoe-api-key")
client = EntsoeRawClient(api_key=api_key, retry_count=3, retry_delay=10, timeout=120)

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:6]
now_utc = pd.Timestamp.now(tz="UTC")
today_utc = now_utc.normalize()
wanted = [d.strip() for d in datasets.split(",") if d.strip()]
print("run_id:", RUN_ID, "| mode:", mode, "| datasets:", wanted)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Areas per dataset for ACTIVE countries (load, generation, prices)
area_jobs = spark.sql("""
    SELECT da.dataset, a.area_code
    FROM dataset_area da
    JOIN area    a ON a.area_code    = da.area_code
    JOIN country c ON c.country_code = a.country_code
    WHERE da.is_enabled AND c.is_active
      AND da.dataset IN ('actual_load', 'actual_generation', 'day_ahead_price')
""").collect()

# Borders where at least one side is an active country (both directions are separate rows)
flow_jobs = spark.sql("""
    SELECT i.from_area_code, i.to_area_code
    FROM interconnector i
    JOIN area    a_from ON a_from.area_code    = i.from_area_code
    JOIN area    a_to   ON a_to.area_code      = i.to_area_code
    JOIN country c_from ON c_from.country_code = a_from.country_code
    JOIN country c_to   ON c_to.country_code   = a_to.country_code
    WHERE i.is_enabled AND (c_from.is_active OR c_to.is_active)
""").collect()

targets = [(r.dataset, r.area_code) for r in area_jobs if r.dataset in wanted]
if "crossborder_flow" in wanted:
    targets += [("crossborder_flow", f"{r.from_area_code}>{r.to_area_code}") for r in flow_jobs]

print(f"{len(targets)} dataset/area targets")
print(pd.DataFrame(targets, columns=["dataset", "area_key"]).groupby("dataset").size().to_string())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def window_end(dataset):
    if mode == "test":
        return today_utc
    # Day-ahead prices for tomorrow are published around midday CET
    return today_utc + pd.Timedelta(days=2 if dataset == "day_ahead_price" else 1)

watermarks = {}
if mode == "incremental":
    for r in spark.sql("SELECT dataset, area_key, loaded_until_utc FROM ctl_watermark WHERE source = 'entsoe'").collect():
        watermarks[(r.dataset, r.area_key)] = pd.Timestamp(r.loaded_until_utc).tz_localize("UTC")

def window_start(dataset, area_key):
    if mode == "test":
        return today_utc - pd.Timedelta(days=test_days)
    if mode == "incremental" and (dataset, area_key) in watermarks:
        return watermarks[(dataset, area_key)] - pd.Timedelta(days=repull_days)
    return pd.Timestamp(backfill_start, tz="UTC")

def month_chunks(start, end):
    chunks, cur = [], start
    while cur < end:
        nxt = min((cur + pd.offsets.MonthBegin(1)).normalize(), end)
        chunks.append((cur, nxt))
        cur = nxt
    return chunks

jobs = [(ds, key, s, e)
        for ds, key in targets
        for s, e in month_chunks(window_start(ds, key), window_end(ds))]
print(f"{len(jobs)} API calls planned")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Resume support: a backfill skips monthly chunks that an earlier run already downloaded
if mode == "backfill":
    done = spark.sql("""
        SELECT dataset, area_key, request_start_utc, request_end_utc
        FROM ctl_ingest_log
        WHERE source = 'entsoe' AND status IN ('ok', 'no_data')
    """).collect()
    done_keys = {(r.dataset, r.area_key,
                  pd.Timestamp(r.request_start_utc).tz_localize("UTC"),
                  pd.Timestamp(r.request_end_utc).tz_localize("UTC")) for r in done}
    before = len(jobs)
    jobs = [j for j in jobs if (j[0], j[1], j[2], j[3]) not in done_keys]
    print(f"Resume: {before - len(jobs)} chunks already done, {len(jobs)} left to download")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def call_api(dataset, area_key, start, end):
    if dataset == "actual_load":
        return client.query_load(area_key, start=start, end=end)
    if dataset == "actual_generation":
        return client.query_generation(area_key, start=start, end=end, psr_type=None)
    if dataset == "day_ahead_price":
        return client.query_day_ahead_prices(area_key, start=start, end=end)
    if dataset == "crossborder_flow":
        from_area, to_area = area_key.split(">")
        return client.query_crossborder_flows(from_area, to_area, start=start, end=end)
    raise ValueError(f"Unknown dataset {dataset}")

def fetch_one(job):
    dataset, area_key, start, end = job
    safe_key = area_key.replace(">", "_to_")
    path = (f"Files/raw/entsoe/{dataset}/{safe_key}/{start:%Y}/{start:%m}/"
            f"{dataset}__{safe_key}__{start:%Y%m%dT%H%M}_{end:%Y%m%dT%H%M}__{RUN_ID}.xml")
    rec = dict(run_id=RUN_ID, source="entsoe", dataset=dataset, area_key=area_key,
               request_start_utc=start.to_pydatetime(), request_end_utc=end.to_pydatetime(),
               status=None, file_path=None, bytes=None, points=None, error_message=None)
    try:
        xml_text = call_api(dataset, area_key, start, end)
        notebookutils.fs.put(path, xml_text, True)
        rec.update(status="ok", file_path=path,
                   bytes=len(xml_text.encode("utf-8")), points=xml_text.count("<Point>"))
    except NoMatchingDataError:
        rec.update(status="no_data")
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

results, buffer = [], []
with ThreadPoolExecutor(max_workers=max_workers) as pool:
    futures = [pool.submit(fetch_one, j) for j in jobs]
    for i, fut in enumerate(as_completed(futures), 1):
        rec = fut.result()
        results.append(rec)
        buffer.append(rec)
        if len(buffer) >= 50:
            write_log(buffer)
            buffer = []
            print(f"{i}/{len(jobs)} calls done")
write_log(buffer)
print(f"Finished: {len(results)} calls")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if mode == "test":
    print("Test mode: watermark not updated")

elif mode == "backfill":
    # Use the log of ALL runs: a key advances only if the latest attempt of every
    # monthly backfill chunk succeeded (ok / no_data)
    spark.sql("""
        MERGE INTO ctl_watermark t
        USING (
            WITH latest AS (
                SELECT dataset, area_key, request_start_utc, request_end_utc, status,
                       ROW_NUMBER() OVER (PARTITION BY dataset, area_key, request_start_utc, request_end_utc
                                          ORDER BY ingested_at_utc DESC) AS rn
                FROM ctl_ingest_log
                WHERE source = 'entsoe'
                  AND date_format(request_start_utc, 'dd HH:mm') = '01 00:00'   -- monthly backfill chunks only
            )
            SELECT 'entsoe' AS source, dataset, area_key,
                   LEAST(MAX(request_end_utc), current_timestamp()) AS loaded_until_utc,
                   current_timestamp() AS updated_at_utc
            FROM latest
            WHERE rn = 1
            GROUP BY dataset, area_key
            HAVING SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) = 0
        ) s
        ON t.source = s.source AND t.dataset = s.dataset AND t.area_key = s.area_key
        WHEN MATCHED THEN UPDATE SET t.loaded_until_utc = s.loaded_until_utc,
                                     t.updated_at_utc  = s.updated_at_utc
        WHEN NOT MATCHED THEN INSERT *
    """)
    spark.sql("""SELECT dataset, COUNT(*) AS keys, MIN(loaded_until_utc) AS min_loaded_until
                 FROM ctl_watermark WHERE source = 'entsoe' GROUP BY dataset""").show(truncate=False)
    
else:  # incremental
    res = pd.DataFrame(results)
    summary = (res.assign(is_error=res.status.eq("error"))
                  .groupby(["dataset", "area_key"])
                  .agg(errors=("is_error", "sum"), max_end=("request_end_utc", "max"))
                  .reset_index())
    ok = summary[summary.errors == 0]
    wm_rows = [(r.dataset, r.area_key, min(pd.Timestamp(r.max_end), now_utc).to_pydatetime())
               for r in ok.itertuples()]
    if wm_rows:
        spark.createDataFrame(wm_rows, "dataset string, area_key string, loaded_until_utc timestamp") \
             .createOrReplaceTempView("wm_updates")
        spark.sql("""
            MERGE INTO ctl_watermark t
            USING (SELECT 'entsoe' AS source, dataset, area_key, loaded_until_utc,
                          current_timestamp() AS updated_at_utc FROM wm_updates) s
            ON t.source = s.source AND t.dataset = s.dataset AND t.area_key = s.area_key
            WHEN MATCHED THEN UPDATE SET t.loaded_until_utc = s.loaded_until_utc,
                                         t.updated_at_utc  = s.updated_at_utc
            WHEN NOT MATCHED THEN INSERT *
        """)
    print(f"Watermark advanced for {len(wm_rows)} keys")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

res = pd.DataFrame(results)
if res.empty:
    summary = {"calls": 0, "note": "nothing to download (all chunks already done)"}
    print(summary["note"])
else:
    counts = res.groupby(["dataset", "status"]).size().unstack(fill_value=0)
    print(counts.to_string())
    errors = res[res.status == "error"]
    print(f"\n{len(errors)} errors")
    if len(errors):
        print(errors[["dataset", "area_key", "error_message"]].head(50).to_string())
    summary = {"calls": int(len(res)),
               "ok": int((res.status == "ok").sum()),
               "no_data": int((res.status == "no_data").sum()),
               "errors": int(len(errors))}

# The exit value appears in the pipeline's Notebook activity output
notebookutils.notebook.exit(json.dumps(summary))

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
