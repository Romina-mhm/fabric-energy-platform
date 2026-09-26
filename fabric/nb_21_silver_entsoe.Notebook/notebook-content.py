# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4a9c15b0-cd48-49fc-85c6-fdd5dcc0b185",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "a9b50eeb-8564-4e09-ac18-87c3583d27a3",
# META       "known_lakehouses": [
# META         {
# META           "id": "4a9c15b0-cd48-49fc-85c6-fdd5dcc0b185"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import re
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window

spark.conf.set("spark.sql.session.timeZone", "UTC")

# Build OneLake paths from IDs (works in pipelines, survives renames)
ws_id = notebookutils.runtime.context["currentWorkspaceId"]
lh = notebookutils.lakehouse.get("lh_bronze")
lh_id = lh["id"] if isinstance(lh, dict) else lh.id
BRONZE_ABFS = f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{lh_id}"
LOG_PATH = f"{BRONZE_ABFS}/Tables/ctl_ingest_log"      # no dbo folder in lh_bronze

log = spark.read.format("delta").load(LOG_PATH)
(log.filter(F.col("source") == "entsoe")
    .groupBy("dataset", "status").count()
    .orderBy("dataset", "status")
    .show(truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

_RES_RE = re.compile(r"^PT(?:(\d+)H)?(?:(\d+)M)?$")

def _res_minutes(res):
    m = _RES_RE.match(res.strip())
    if not m or not (m.group(1) or m.group(2)):
        raise ValueError(f"Unsupported resolution: {res}")
    return int(m.group(1) or 0) * 60 + int(m.group(2) or 0)

def _ts(s):
    return datetime.fromisoformat(s.strip().replace("Z", "+00:00"))

def _txt(el, path):
    x = el.find(path)
    return x.text.strip() if x is not None and x.text else None

def parse_entsoe_xml(content, dataset, area_key, source_file):
    """Any ENTSO-E document -> list of
    (dataset, area_key, psr_type_code, direction, ts_utc, resolution_minutes, value, source_file)"""
    root = ET.fromstring(content)
    for el in root.iter():                      # strip XML namespaces (they differ per document type)
        if isinstance(el.tag, str):
            el.tag = el.tag.rsplit("}", 1)[-1]
    value_tag = "price.amount" if dataset == "day_ahead_price" else "quantity"
    rows = []
    for series in root.findall("TimeSeries"):
        psr = _txt(series, "MktPSRType/psrType")
        curve = _txt(series, "curveType") or "A01"
        direction = None
        if dataset == "actual_generation":
            direction = "consumption" if series.find("outBiddingZone_Domain.mRID") is not None else "generation"
        for period in series.findall("Period"):
            start = _ts(_txt(period, "timeInterval/start"))
            end = _ts(_txt(period, "timeInterval/end"))
            res = _res_minutes(_txt(period, "resolution"))
            step = timedelta(minutes=res)
            n_slots = int((end - start) / step)
            points = {}
            for pt in period.findall("Point"):
                val = _txt(pt, value_tag)
                if val is not None:
                    points[int(_txt(pt, "position"))] = float(val)
            if curve == "A03":                  # omitted position = same value as previous -> forward-fill
                last = None
                positions = range(1, n_slots + 1)
            else:
                positions = sorted(p for p in points if p <= n_slots)
            for pos in positions:
                if curve == "A03":
                    last = points.get(pos, last)
                    val = last
                else:
                    val = points[pos]
                if val is None:
                    continue
                rows.append((dataset, area_key, psr, direction,
                             start + (pos - 1) * step, res, val, source_file))
    return rows

PARSED_SCHEMA = T.StructType([
    T.StructField("dataset", T.StringType(), False),
    T.StructField("area_key", T.StringType(), False),
    T.StructField("psr_type_code", T.StringType(), True),
    T.StructField("direction", T.StringType(), True),
    T.StructField("ts_utc", T.TimestampType(), False),
    T.StructField("resolution_minutes", T.IntegerType(), False),
    T.StructField("value", T.DoubleType(), False),
    T.StructField("source_file", T.StringType(), False),
])
print("parser ready")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ok = log.filter((F.col("source") == "entsoe") & (F.col("status") == "ok"))
w_new = Window.partitionBy("dataset").orderBy(F.col("request_start_utc").desc())
w_old = Window.partitionBy("dataset").orderBy(F.col("request_start_utc").asc())
samples = (ok.withColumn("rn_new", F.row_number().over(w_new))
             .withColumn("rn_old", F.row_number().over(w_old))
             .filter("rn_new = 1 OR rn_old = 1")
             .select("dataset", "area_key", "file_path", "request_start_utc", "request_end_utc")
             .orderBy("dataset", "request_start_utc")
             .collect())

for s in samples:
    content = (spark.read.format("binaryFile")
                    .load(f"{BRONZE_ABFS}/{s.file_path}")
                    .select("content").first()[0])
    rows = parse_entsoe_xml(bytes(content), s.dataset, s.area_key, s.file_path)
    ts = [r[4] for r in rows]
    series = sorted({(r[2], r[3]) for r in rows}, key=str)
    print(f"\n{s.dataset} | {s.area_key} | request {s.request_start_utc} -> {s.request_end_utc}")
    print(f"  rows={len(rows)}  resolutions={sorted({r[5] for r in rows})}  "
          f"ts {min(ts) if ts else None} -> {max(ts) if ts else None}")
    print(f"  series (psr, direction): {len(series)} -> {series[:8]}")
    for r in rows[:2]:
        print("  sample:", r[2], r[3], r[4], r[5], r[6])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

files = (ok.filter(F.col("area_key").startswith("NL"))
           .withColumn("file_name", F.element_at(F.split("file_path", "/"), -1))
           .select("dataset", "area_key", "file_path", "file_name"))
paths = [f"{BRONZE_ABFS}/{r.file_path}" for r in files.select("file_path").collect()]
print("files:", len(paths))

bin_df = (spark.read.format("binaryFile").load(paths)
               .withColumn("file_name", F.element_at(F.split("path", "/"), -1))
               .select("file_name", "content"))
joined = bin_df.join(files, "file_name")

parsed = spark.createDataFrame(
    joined.rdd.flatMap(lambda r: parse_entsoe_xml(bytes(r.content), r.dataset, r.area_key, r.file_path)),
    PARSED_SCHEMA).cache()

(parsed.groupBy("dataset", "resolution_minutes")
       .agg(F.count("*").alias("rows"), F.min("ts_utc").alias("min_ts"), F.max("ts_utc").alias("max_ts"))
       .orderBy("dataset", "resolution_minutes").show(truncate=False))

keys = ["dataset", "area_key", "psr_type_code", "direction", "ts_utc", "resolution_minutes"]
dupes = parsed.groupBy(keys).count().filter("count > 1").count()
print("duplicate keys (expected > 0 from overlapping re-pulls):", dupes)
parsed.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
