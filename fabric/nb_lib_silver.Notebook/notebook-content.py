# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql import functions as F
from delta.tables import DeltaTable

LINEAGE = {"_source_file", "_bronze_ingested_at", "_silver_updated_at"}

def align(df, table):
    """Select + cast the target table's columns by name; fail loudly if one is missing."""
    target = spark.table(table).schema
    missing = [f.name for f in target if f.name not in df.columns]
    if missing:
        raise ValueError(f"{table}: source is missing columns {missing}")
    return df.select([F.col(f.name).cast(f.dataType).alias(f.name) for f in target])

def merge_into(table, keys, src, extra_guard=None):
    """Conditional MERGE; reports metrics only if a new Delta version was committed."""
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
