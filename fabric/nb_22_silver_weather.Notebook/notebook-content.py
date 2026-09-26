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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark.conf.set("spark.sql.session.timeZone", "UTC")

ws_id = notebookutils.runtime.context["currentWorkspaceId"]
lh = notebookutils.lakehouse.get("lh_bronze")
lh_id = lh["id"] if isinstance(lh, dict) else lh.id
BRONZE_ABFS = f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{lh_id}"

VARS = ["temperature_2m", "wind_speed_10m", "wind_speed_100m",
        "shortwave_radiation", "cloud_cover", "precipitation"]
COUNTRY_VARS = ["temperature_2m", "wind_speed_100m", "shortwave_radiation", "cloud_cover", "precipitation"]
LINEAGE = {"_source_file", "_bronze_ingested_at", "_silver_updated_at"}
STATE_KEY = "open_meteo_weather"

log = spark.read.format("delta").load(f"{BRONZE_ABFS}/Tables/ctl_ingest_log")
loc = (spark.read.format("delta").load(f"{BRONZE_ABFS}/Tables/weather_location")
            .select("location_code", "country_code", F.col("weight").cast("double").alias("weight")))

st = spark.table("ctl_silver_state").filter(F.col("dataset") == STATE_KEY).collect()
since = st[0].processed_until_ingested_at if st else None

files = log.filter((F.col("source") == "open_meteo") & (F.col("status") == "ok")
                   & F.col("dataset").startswith("weather_hourly_"))
if mode == "incremental" and since is not None:
    files = files.filter(F.col("ingested_at_utc") > F.lit(since))
files = (files.withColumn("file_name", F.element_at(F.split("file_path", "/"), -1))
              .withColumn("data_source", F.when(F.col("dataset").contains("archive"), "archive")
                                          .otherwise("forecast"))
              .select("area_key", "file_path", "file_name", "ingested_at_utc", "data_source"))
file_rows = files.collect()
print("mode:", mode, "| since:", since, "| files:", len(file_rows))
files.groupBy("data_source").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def align(df, table):
    target = spark.table(table).schema
    missing = [f.name for f in target if f.name not in df.columns]
    if missing:
        raise ValueError(f"{table}: source is missing columns {missing}")
    return df.select([F.col(f.name).cast(f.dataType).alias(f.name) for f in target])

def merge_into(table, keys, src, extra_guard=None):
    v_before = DeltaTable.forName(spark, table).history(1).select("version").first()[0]
    target_cols = [f.name for f in spark.table(table).schema]
    value_cols = [c for c in target_cols if c not in keys and c not in LINEAGE]
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    changed = "(" + " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in value_cols) + ")"
    cond = f"{extra_guard} AND {changed}" if extra_guard else changed
    (DeltaTable.forName(spark, table).alias("t")
        .merge(src.alias("s"), on)
        .whenMatchedUpdate(condition=cond, set={c: f"s.{c}" for c in target_cols if c not in keys})
        .whenNotMatchedInsertAll()
        .execute())
    # only trust metrics if this MERGE actually committed a new version
    h = DeltaTable.forName(spark, table).history(1).select("version", "operationMetrics").first()
    if h.version == v_before:
        return {"numTargetRowsInserted": "0", "numTargetRowsUpdated": "0",
                "numSourceRows": "no commit (nothing changed)"}
    m = h.operationMetrics
    return {k: m.get(k) for k in ("numTargetRowsInserted", "numTargetRowsUpdated", "numSourceRows")}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary = {"mode": mode, "files": len(file_rows)}

if file_rows:
    raw = (spark.read.option("multiLine", True)
                .json([f"{BRONZE_ABFS}/{r.file_path}" for r in file_rows])
                .withColumn("file_name", F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1)))

    tzs = [r.timezone for r in raw.select("timezone").distinct().collect()]
    print("timezones in files:", tzs)
    if not set(tzs) <= {"GMT", "UTC"}:
        raise ValueError(f"Expected GMT/UTC timestamps from Open-Meteo, got {tzs}")

    # parallel arrays -> one row per hour
    flat = raw.select("file_name", F.col("hourly.time").alias("time"),
                      *[F.col(f"hourly.{v}").alias(v) for v in VARS])
    hourly = (flat.select("file_name", F.explode(F.arrays_zip("time", *VARS)).alias("z"))
                  .select("file_name", "z.*"))

    parsed = (hourly.join(F.broadcast(files), "file_name")
                    .withColumnRenamed("area_key", "location_code")
                    .join(F.broadcast(loc.select("location_code", "country_code")), "location_code", "left")
                    .withColumn("ts_utc", F.to_timestamp("time", "yyyy-MM-dd'T'HH:mm"))
                    .select("location_code", "country_code", "ts_utc",
                            *[F.col(v).cast("double").alias(v) for v in VARS],
                            "data_source", "file_path", "ingested_at_utc")
                    .filter(" OR ".join(f"{v} IS NOT NULL" for v in VARS)))

    unknown = parsed.filter("country_code IS NULL").select("location_code").distinct().collect()
    if unknown:
        raise ValueError(f"Locations missing in weather_location: {[r.location_code for r in unknown]}")

    # archive beats forecast; within the same source the latest ingest wins
    w = (Window.partitionBy("location_code", "ts_utc")
               .orderBy(F.when(F.col("data_source") == "archive", 0).otherwise(1),
                        F.col("ingested_at_utc").desc(), F.col("file_path").desc()))
    latest = (parsed.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
                    .withColumnRenamed("file_path", "_source_file")
                    .withColumnRenamed("ingested_at_utc", "_bronze_ingested_at")
                    .withColumn("_silver_updated_at", F.current_timestamp())
                    .cache())

    # a forecast must never overwrite an archive (observed) value
    summary["silver_weather_hourly"] = merge_into(
        "silver_weather_hourly", ["location_code", "ts_utc"], align(latest, "silver_weather_hourly"),
        extra_guard="NOT (t.data_source = 'archive' AND s.data_source = 'forecast')")
    print("hourly:", summary["silver_weather_hourly"])

    # recompute country-hours only for the days this batch touched
    affected = latest.select("country_code", F.to_date("ts_utc").alias("d")).distinct()
    h = (spark.table("silver_weather_hourly")
              .withColumn("d", F.to_date("ts_utc"))
              .join(F.broadcast(affected), ["country_code", "d"], "left_semi")
              .join(F.broadcast(loc.select("location_code", "weight")), "location_code"))

    def wavg(c):   # weighted mean over the locations that have a value (renormalises weights)
        return (F.sum(F.col(c) * F.col("weight"))
                / F.sum(F.when(F.col(c).isNotNull(), F.col("weight")))).alias(c)

    country = (h.groupBy("country_code", "ts_utc")
                .agg(*[wavg(c) for c in COUNTRY_VARS],
                     (F.sum(F.when(F.col("data_source") == "archive", F.col("weight")).otherwise(0.0))
                      / F.sum("weight")).alias("archive_share"))
                .withColumn("_silver_updated_at", F.current_timestamp()))

    summary["silver_weather_country_hourly"] = merge_into(
        "silver_weather_country_hourly", ["country_code", "ts_utc"],
        align(country, "silver_weather_country_hourly"))
    print("country:", summary["silver_weather_country_hourly"])
    latest.unpersist()

    max_ing = max(r.ingested_at_utc for r in file_rows)
    spark.createDataFrame([(STATE_KEY, max_ing)], "dataset string, ts timestamp").createOrReplaceTempView("new_state")
    spark.sql("""
        MERGE INTO ctl_silver_state t
        USING new_state s ON t.dataset = s.dataset
        WHEN MATCHED THEN UPDATE SET t.processed_until_ingested_at = s.ts, t.updated_at_utc = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (dataset, processed_until_ingested_at, updated_at_utc)
                              VALUES (s.dataset, s.ts, current_timestamp())
    """)
else:
    print("nothing new")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for t in ["silver_weather_hourly", "silver_weather_country_hourly"]:
    summary[t + "_rows"] = spark.table(t).count()
print(json.dumps(summary, indent=2, default=str))
notebookutils.notebook.exit(json.dumps(summary, default=str))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
  SELECT country_code, data_source, COUNT(*) AS rows_, MIN(ts_utc) AS first_ts, MAX(ts_utc) AS last_ts
  FROM silver_weather_hourly GROUP BY country_code, data_source ORDER BY country_code, data_source
""").show(truncate=False)

print("duplicate keys:", spark.table("silver_weather_hourly")
      .groupBy("location_code", "ts_utc").count().filter("count > 1").count())

spark.sql("""
  SELECT country_code, month(ts_utc) AS m, ROUND(AVG(temperature_2m), 1) AS avg_temp_c,
         ROUND(AVG(shortwave_radiation), 0) AS avg_solar_wm2, ROUND(AVG(wind_speed_100m), 1) AS avg_wind100_kmh
  FROM silver_weather_country_hourly
  WHERE year(ts_utc) = 2025 AND month(ts_utc) IN (1, 7)
  GROUP BY country_code, month(ts_utc) ORDER BY country_code, m
""").show()

spark.sql("""
  SELECT country_code, MIN(archive_share) AS min_share, MAX(archive_share) AS max_share
  FROM silver_weather_country_hourly GROUP BY country_code ORDER BY country_code
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
