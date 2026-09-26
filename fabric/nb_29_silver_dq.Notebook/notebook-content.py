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

run_optimize = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json, uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")
ws_id = notebookutils.runtime.context["currentWorkspaceId"]
lh = notebookutils.lakehouse.get("lh_bronze")
lh_id = lh["id"] if isinstance(lh, dict) else lh.id
BRONZE_ABFS = f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{lh_id}"

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:6]

spark.sql("""
CREATE TABLE IF NOT EXISTS dq_check_result (
    run_id STRING, checked_at TIMESTAMP, check_name STRING, table_name STRING,
    scope STRING, metric DOUBLE, threshold STRING, status STRING, details STRING
) USING DELTA
""")

area_codes = [r.area_code for r in spark.read.format("delta")
              .load(f"{BRONZE_ABFS}/Tables/area").select("area_code").collect()]
psr_codes = [r.psr_type_code for r in spark.read.format("delta")
             .load(f"{BRONZE_ABFS}/Tables/production_type").select("psr_type_code").collect()]

results = []   # (check_name, table_name, scope, metric, threshold, status, details)
def add(check, table, scope, metric, threshold, status, details=""):
    results.append((check, table, scope, float(metric), threshold, status, details))

KEYS = {
    "silver_load":       ["area_code", "ts_utc", "resolution_minutes"],
    "silver_generation": ["area_code", "ts_utc", "resolution_minutes", "psr_type_code", "direction"],
    "silver_price":      ["area_code", "ts_utc", "resolution_minutes"],
    "silver_flow":       ["from_area_code", "to_area_code", "ts_utc", "resolution_minutes"],
    "silver_weather_hourly": ["location_code", "ts_utc"],
    "silver_weather_country_hourly": ["country_code", "ts_utc"],
    "silver_holiday":    ["country_code", "holiday_date", "name"],
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for t, keys in KEYS.items():
    d = spark.table(t).groupBy(*keys).count().filter("count > 1").count()
    add("unique_key", t, ",".join(keys), d, "= 0", "PASS" if d == 0 else "FAIL")

for t, cols in [("silver_load", ["area_code"]), ("silver_generation", ["area_code"]),
                ("silver_price", ["area_code"]), ("silver_flow", ["from_area_code", "to_area_code"])]:
    for c in cols:
        bad = [r[0] for r in spark.table(t).select(c).distinct()
               .filter(~F.col(c).isin(area_codes)).collect()]
        add("area_in_reference", t, c, len(bad), "= 0", "PASS" if not bad else "FAIL", ",".join(bad))

bad_psr = [r[0] for r in spark.table("silver_generation").select("psr_type_code").distinct()
           .filter(~F.col("psr_type_code").isin(psr_codes)).collect()]
add("psr_in_reference", "silver_generation", "psr_type_code", len(bad_psr), "= 0",
    "PASS" if not bad_psr else "FAIL", ",".join(bad_psr))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

RANGES = [
    ("silver_load",       "load_mw",        0,     150000),
    ("silver_generation", "power_mw",       0,     100000),
    ("silver_price",      "price_eur_mwh", -500,   4000),     # EU day-ahead harmonised min / max clearing price
    ("silver_flow",       "flow_mw",        0,     20000),
    ("silver_weather_hourly", "temperature_2m", -40, 50),
    ("silver_weather_hourly", "wind_speed_100m", 0,  250),
]
for t, c, lo, hi in RANGES:
    n = spark.table(t).filter((F.col(c) < lo) | (F.col(c) > hi)).count()
    add("value_range", t, c, n, f"[{lo}, {hi}]", "PASS" if n == 0 else "WARN")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def completeness(t, scope_cols):
    df = (spark.table(t)
               .withColumn("d", F.to_date("ts_utc"))
               .filter(F.col("d") < F.date_sub(F.current_date(), 1))          # skip days still arriving
               .select(*scope_cols, "d", "resolution_minutes", "ts_utc").distinct())
    per_day = (df.groupBy(*scope_cols, "d")
                 .agg(F.sum("resolution_minutes").alias("minutes"),
                      F.countDistinct("resolution_minutes").alias("n_res")))
    gaps = per_day.filter("minutes < 1440")
    overlap = per_day.filter("minutes > 1440")
    n_days = per_day.count()
    n_gap, n_overlap = gaps.count(), overlap.count()
    add("completeness_gap_days", t, ",".join(scope_cols), n_gap, "minutes < 1440",
        "PASS" if n_gap == 0 else "WARN",
        f"{n_gap} of {n_days} scope-days incomplete; worst: " +
        "; ".join(f"{'/'.join(str(r[c]) for c in scope_cols)} {r.d} {r.minutes}min"
                  for r in gaps.orderBy("minutes").limit(5).collect()))
    add("completeness_overlap_days", t, ",".join(scope_cols), n_overlap, "minutes > 1440",
        "PASS" if n_overlap == 0 else "INFO",
        f"{n_overlap} scope-days published at more than one resolution")

completeness("silver_load", ["area_code"])
completeness("silver_generation", ["area_code"])
completeness("silver_price", ["area_code"])
completeness("silver_flow", ["from_area_code", "to_area_code"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

now = datetime.now(timezone.utc).replace(tzinfo=None)
FRESH = [("silver_load", 2), ("silver_generation", 2), ("silver_price", 1),
         ("silver_flow", 2), ("silver_weather_hourly", 1)]
for t, max_days in FRESH:
    last = spark.table(t).agg(F.max("ts_utc")).first()[0]
    age_h = (now - last).total_seconds() / 3600 if last else 1e9
    add("freshness_hours", t, "max(ts_utc)", round(age_h, 1), f"<= {max_days * 24} h",
        "PASS" if age_h <= max_days * 24 else "WARN", f"latest ts_utc {last}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

res_df = (spark.createDataFrame(results,
            "check_name string, table_name string, scope string, metric double, threshold string, status string, details string")
               .withColumn("run_id", F.lit(RUN_ID))
               .withColumn("checked_at", F.current_timestamp())
               .select("run_id", "checked_at", "check_name", "table_name", "scope",
                       "metric", "threshold", "status", "details"))
res_df.write.mode("append").saveAsTable("dq_check_result")

for r in results:
    print(f"{r[5]:5} | {r[0]:26} | {r[1]:30} | {r[2]:28} | {r[3]:>10} | {r[6][:120]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

optimize_log = {}
if run_optimize:
    for t in list(KEYS) + ["dq_quarantine", "dq_check_result"]:
        before = spark.sql(f"DESCRIBE DETAIL {t}").select("numFiles").first()[0]
        spark.sql(f"OPTIMIZE {t} VORDER")
        after = spark.sql(f"DESCRIBE DETAIL {t}").select("numFiles").first()[0]
        optimize_log[t] = f"{before} -> {after} files"
        print(f"{t}: {before} -> {after} files")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary = {"run_id": RUN_ID,
           "FAIL": sum(1 for r in results if r[5] == "FAIL"),
           "WARN": sum(1 for r in results if r[5] == "WARN"),
           "checks": len(results), "optimize": optimize_log}
print(json.dumps(summary, indent=2))
notebookutils.notebook.exit(json.dumps(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
  SELECT area_code, resolution_minutes,
         MIN(ts_utc) AS first_ts, MAX(ts_utc) AS last_ts, COUNT(*) AS rows_
  FROM silver_price
  GROUP BY area_code, resolution_minutes
  ORDER BY area_code, resolution_minutes
""").show(40, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
