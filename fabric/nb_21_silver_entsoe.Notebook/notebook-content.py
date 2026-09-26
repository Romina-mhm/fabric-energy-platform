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

mode = "incremental"          # incremental | full
datasets = "actual_load,actual_generation,day_ahead_price,crossborder_flow"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
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
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark import StorageLevel

TARGETS = {
    "actual_load":       ("silver_load",       ["area_code", "ts_utc", "resolution_minutes"]),
    "actual_generation": ("silver_generation", ["area_code", "ts_utc", "resolution_minutes", "psr_type_code", "direction"]),
    "day_ahead_price":   ("silver_price",      ["area_code", "ts_utc", "resolution_minutes"]),
    "crossborder_flow":  ("silver_flow",       ["from_area_code", "to_area_code", "ts_utc", "resolution_minutes"]),
}
LINEAGE = {"_source_file", "_bronze_ingested_at", "_silver_updated_at"}

PARSED_ING_SCHEMA = T.StructType(PARSED_SCHEMA.fields + [
    T.StructField("_bronze_ingested_at", T.TimestampType(), True)])

def parse_or_error(r):
    try:
        return [("ok", x + (r.ingested_at_utc,), None)
                for x in parse_entsoe_xml(bytes(r.content), r.dataset, r.area_key, r.file_path)]
    except Exception as e:
        return [("err", None, (r.dataset, r.file_path, f"{type(e).__name__}: {e}"[:1000]))]

def align(df, table):
    """Select + cast the target table's columns by name; fail loudly if one is missing."""
    target = spark.table(table).schema
    missing = [f.name for f in target if f.name not in df.columns]
    if missing:
        raise ValueError(f"{table}: source is missing columns {missing}")
    return df.select([F.col(f.name).cast(f.dataType).alias(f.name) for f in target])

prod = (spark.read.format("delta").load(f"{BRONZE_ABFS}/Tables/production_type")
             .select("psr_type_code", F.col("lifecycle_gco2e_per_kwh").cast("double").alias("gco2e_per_kwh")))

state = {r.dataset: r.processed_until_ingested_at
         for r in spark.table("ctl_silver_state").collect()}
print("mode:", mode, "| state:", state)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def shape(dataset, df):
    hours = F.col("resolution_minutes") / 60.0
    if dataset == "actual_load":
        return (df.withColumnRenamed("area_key", "area_code")
                  .withColumn("load_mw", F.col("value"))
                  .withColumn("energy_mwh", F.col("value") * hours))
    if dataset == "actual_generation":
        return (df.withColumnRenamed("area_key", "area_code")
                  .join(F.broadcast(prod), "psr_type_code", "left")
                  .withColumn("power_mw", F.col("value"))
                  .withColumn("energy_mwh", F.col("value") * hours)
                  # MWh x gCO2e/kWh = kg CO2e ; lifecycle emissions only for generation
                  .withColumn("co2e_kg", F.when(F.col("direction") == "generation",
                                                F.col("energy_mwh") * F.col("gco2e_per_kwh"))))
    if dataset == "day_ahead_price":
        return (df.withColumnRenamed("area_key", "area_code")
                  .withColumn("price_eur_mwh", F.col("value")))
    if dataset == "crossborder_flow":
        parts = F.split("area_key", ">")
        return (df.withColumn("from_area_code", parts.getItem(0))
                  .withColumn("to_area_code", parts.getItem(1))
                  .withColumn("flow_mw", F.col("value"))
                  .withColumn("energy_mwh", F.col("value") * hours))
    raise ValueError(dataset)

def merge_into(table, keys, src):
    target_cols = [f.name for f in spark.table(table).schema]
    value_cols = [c for c in target_cols if c not in keys and c not in LINEAGE]
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in value_cols)
    (DeltaTable.forName(spark, table).alias("t")
        .merge(src.alias("s"), on)
        .whenMatchedUpdate(condition=changed, set={c: f"s.{c}" for c in target_cols if c not in keys})
        .whenNotMatchedInsertAll()
        .execute())
    m = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").select("operationMetrics").first()[0]
    return {k: m.get(k) for k in ("numTargetRowsInserted", "numTargetRowsUpdated", "numSourceRows")}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary = {"mode": mode, "datasets": {}}
ok_log = log.filter((F.col("source") == "entsoe") & (F.col("status") == "ok"))

for dataset in [d.strip() for d in datasets.split(",") if d.strip()]:
    table, keys = TARGETS[dataset]
    files = ok_log.filter(F.col("dataset") == dataset)
    since = state.get(dataset)
    if mode == "incremental" and since is not None:
        files = files.filter(F.col("ingested_at_utc") > F.lit(since))
    files = (files.withColumn("file_name", F.element_at(F.split("file_path", "/"), -1))
                  .select("dataset", "area_key", "file_path", "file_name", "ingested_at_utc"))
    file_rows = files.collect()
    if not file_rows:
        print(f"{dataset}: nothing new")
        summary["datasets"][dataset] = {"files": 0}
        continue
    max_ing = max(r.ingested_at_utc for r in file_rows)

    bin_df = (spark.read.format("binaryFile")
                   .load([f"{BRONZE_ABFS}/{r.file_path}" for r in file_rows])
                   .withColumn("file_name", F.element_at(F.split("path", "/"), -1))
                   .select("file_name", "content"))
    results = bin_df.join(files, "file_name").repartition(32).rdd.flatMap(parse_or_error) \
                 .persist(StorageLevel.MEMORY_AND_DISK)

    errors = results.filter(lambda x: x[0] == "err").map(lambda x: x[2]).collect()
    if errors:
        (spark.createDataFrame(errors, "dataset string, source_file string, details string")
              .select(F.lit(table).alias("table_name"),
                      F.col("source_file").alias("record_key"),
                      F.lit("xml_parse_error").alias("rule_name"),
                      "details", "source_file",
                      F.current_timestamp().alias("detected_at"))
              .transform(lambda d: align(d, "dq_quarantine"))
              .write.mode("append").saveAsTable("dq_quarantine"))

    parsed = spark.createDataFrame(results.filter(lambda x: x[0] == "ok").map(lambda x: x[1]),
                                   PARSED_ING_SCHEMA)
    # latest Bronze ingest wins per business key (re-pulls and overlapping price days)
    w = (Window.partitionBy("area_key", "ts_utc", "resolution_minutes", "psr_type_code", "direction")
               .orderBy(F.col("_bronze_ingested_at").desc(), F.col("source_file").desc()))
    latest = parsed.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

    src = (shape(dataset, latest)
             .withColumn("_source_file", F.col("source_file"))
             .withColumn("_silver_updated_at", F.current_timestamp()))
    src = align(src, table)
    metrics = merge_into(table, keys, src)
    results.unpersist()

    spark.createDataFrame([(dataset, max_ing)], "dataset string, ts timestamp").createOrReplaceTempView("new_state")
    spark.sql("""
        MERGE INTO ctl_silver_state t
        USING new_state s ON t.dataset = s.dataset
        WHEN MATCHED THEN UPDATE SET t.processed_until_ingested_at = s.ts, t.updated_at_utc = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (dataset, processed_until_ingested_at, updated_at_utc)
                              VALUES (s.dataset, s.ts, current_timestamp())
    """)
    summary["datasets"][dataset] = {"files": len(file_rows), "parse_errors": len(errors),
                                    **{k: str(v) for k, v in metrics.items()}}
    print(dataset, summary["datasets"][dataset])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
for t in ["silver_load", "silver_generation", "silver_price", "silver_flow"]:
    summary[t] = spark.table(t).count()
print(json.dumps(summary, indent=2, default=str))
notebookutils.notebook.exit(json.dumps(summary, default=str))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1) no duplicate business keys
for t, (_, keys) in [(v[0], v) for v in TARGETS.values()]:
    d = spark.table(t).groupBy(*keys).count().filter("count > 1").count()
    print(f"{t}: duplicate keys = {d}")

# 2) generation rows without CO2e -> should only be codes with NULL factor (oil, waste, other, storage...)
spark.sql("""
  SELECT g.psr_type_code, COUNT(*) AS rows_without_co2e
  FROM silver_generation g
  WHERE g.direction = 'generation' AND g.co2e_kg IS NULL
  GROUP BY g.psr_type_code ORDER BY rows_without_co2e DESC
""").show()

# 3) sanity: NL, one day, generation mix and carbon intensity
spark.sql("""
  SELECT SUM(energy_mwh) AS gen_mwh,
         SUM(co2e_kg) / SUM(CASE WHEN co2e_kg IS NOT NULL THEN energy_mwh END) AS g_per_kwh
  FROM silver_generation
  WHERE area_code = 'NL' AND direction = 'generation'
    AND ts_utc >= '2025-06-15' AND ts_utc < '2025-06-16'
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
  SELECT area_code, resolution_minutes, COUNT(*) AS rows_, MIN(ts_utc) AS first_ts, MAX(ts_utc) AS last_ts
  FROM silver_load GROUP BY area_code, resolution_minutes ORDER BY area_code, resolution_minutes
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DESCRIBE HISTORY silver_load LIMIT 3") \
     .select("version", "timestamp", "operation", "operationMetrics.numTargetRowsInserted",
             "operationMetrics.numTargetRowsUpdated") \
     .show(truncate=False)
print("silver_load rows:", spark.table("silver_load").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
