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

import json
from pyspark.sql import functions as F
from delta.tables import DeltaTable

spark.conf.set("spark.sql.session.timeZone", "UTC")
ws_id = notebookutils.runtime.context["currentWorkspaceId"]
lh = notebookutils.lakehouse.get("lh_bronze")
lh_id = lh["id"] if isinstance(lh, dict) else lh.id
BRONZE_ABFS = f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{lh_id}"

def list_json(path):
    out = []
    for f in notebookutils.fs.ls(path):
        if f.isDir:
            out += list_json(f.path)
        elif f.name.endswith(".json"):
            out.append(f.path)
    return out

paths = list_json(f"{BRONZE_ABFS}/Files/raw/holidays")
print("files:", len(paths))

records = []
for p in paths:
    text = notebookutils.fs.head(p, 10_000_000).lstrip("\ufeff")   # Copy activity writes a UTF-8 BOM
    try:
        data = json.loads(text)                     # one JSON array
    except json.JSONDecodeError:
        data = [json.loads(l) for l in text.splitlines() if l.strip()]   # one object per line
    if isinstance(data, dict):
        data = [data]
    rel = "Files/" + p.split("/Files/", 1)[1]
    for h in data:
        records.append((h.get("countryCode"), h.get("date"), h.get("localName"),
                        h.get("name"), h.get("global"), rel))

raw = spark.createDataFrame(records,
        "country_code string, date_str string, local_name string, name string, is_nationwide boolean, _source_file string")
print("holiday rows read:", raw.count())

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

typed = raw.withColumn("holiday_date", F.to_date("date_str", "yyyy-MM-dd"))

bad = typed.filter("country_code IS NULL OR holiday_date IS NULL OR name IS NULL")
n_bad = bad.count()
if n_bad:
    (bad.select(F.lit("silver_holiday").alias("table_name"),
                F.concat_ws("|", "country_code", "date_str", "name").alias("record_key"),
                F.lit("missing_required_field").alias("rule_name"),
                F.lit("country_code, date or name is null").alias("details"),
                F.col("_source_file").alias("source_file"),
                F.current_timestamp().alias("detected_at"))
        .transform(lambda d: align(d, "dq_quarantine"))
        .write.mode("append").saveAsTable("dq_quarantine"))

src = (typed.filter("country_code IS NOT NULL AND holiday_date IS NOT NULL AND name IS NOT NULL")
            .dropDuplicates(["country_code", "holiday_date", "name"])
            .withColumn("is_nationwide", F.coalesce("is_nationwide", F.lit(True)))
            .withColumn("_silver_updated_at", F.current_timestamp()))
src = align(src, "silver_holiday")

v_before = DeltaTable.forName(spark, "silver_holiday").history(1).select("version").first()[0]

(DeltaTable.forName(spark, "silver_holiday").alias("t")
    .merge(src.alias("s"),
           "t.country_code = s.country_code AND t.holiday_date = s.holiday_date AND t.name = s.name")
    .whenMatchedUpdate(
        condition="NOT (t.local_name <=> s.local_name) OR NOT (t.is_nationwide <=> s.is_nationwide)",
        set={"local_name": "s.local_name", "is_nationwide": "s.is_nationwide",
             "_source_file": "s._source_file", "_silver_updated_at": "s._silver_updated_at"})
    .whenNotMatchedInsertAll()
    .whenNotMatchedBySourceDelete()          # source is a full snapshot -> remove vanished holidays
    .execute())

# only trust metrics if this MERGE actually committed a new version
h = DeltaTable.forName(spark, "silver_holiday").history(1).select("version", "operationMetrics").first()
m = h.operationMetrics if h.version != v_before else {
        "numTargetRowsInserted": "0", "numTargetRowsUpdated": "0",
        "numTargetRowsDeleted": "0", "numSourceRows": "no commit (nothing changed)"}

summary = {"files": len(paths), "quarantined": n_bad,
           **{k: m.get(k) for k in ("numTargetRowsInserted", "numTargetRowsUpdated",
                                    "numTargetRowsDeleted", "numSourceRows")},
           "silver_holiday_rows": spark.table("silver_holiday").count()}
print(json.dumps(summary, indent=2, default=str))
notebookutils.notebook.exit(json.dumps(summary, default=str))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
  SELECT country_code, year(holiday_date) AS yr,
         COUNT(*) AS holidays, SUM(CASE WHEN is_nationwide THEN 1 ELSE 0 END) AS nationwide
  FROM silver_holiday GROUP BY country_code, year(holiday_date) ORDER BY country_code, yr
""").show(50)

spark.sql("""
  SELECT holiday_date, local_name, name, is_nationwide
  FROM silver_holiday WHERE country_code = 'NL' AND year(holiday_date) = 2025 ORDER BY holiday_date
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DESCRIBE HISTORY silver_holiday LIMIT 3") \
     .select("version", "timestamp", "operation",
             "operationMetrics.numTargetRowsInserted",
             "operationMetrics.numTargetRowsUpdated",
             "operationMetrics.numTargetRowsDeleted") \
     .show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
