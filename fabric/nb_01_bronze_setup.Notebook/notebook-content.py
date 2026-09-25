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

for folder in ["Files/raw/entsoe", "Files/raw/weather", "Files/raw/holidays"]:
    notebookutils.fs.mkdirs(folder)
    print("ready:", folder)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS ctl_ingest_log (
    run_id            STRING    COMMENT 'One id per notebook run',
    source            STRING    COMMENT 'entsoe | open_meteo | nager_date',
    dataset           STRING    COMMENT 'actual_load | actual_generation | day_ahead_price | crossborder_flow | weather_hourly | public_holidays',
    area_key          STRING    COMMENT 'Area code, flow pair FROM>TO, or weather location',
    request_start_utc TIMESTAMP,
    request_end_utc   TIMESTAMP,
    status            STRING    COMMENT 'ok | no_data | error',
    file_path         STRING    COMMENT 'Where the raw response was saved',
    bytes             BIGINT,
    points            INT       COMMENT 'Number of data points in the response (sanity check)',
    error_message     STRING,
    ingested_at_utc   TIMESTAMP
) USING DELTA
COMMENT 'Bronze audit log: every download attempt, successful or not'
""")
print("ctl_ingest_log ready")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS ctl_watermark (
    source            STRING,
    dataset           STRING,
    area_key          STRING,
    loaded_until_utc  TIMESTAMP COMMENT 'Data is loaded up to (not including) this moment',
    updated_at_utc    TIMESTAMP
) USING DELTA
COMMENT 'Incremental-load state per source, dataset and area'
""")
print("ctl_watermark ready")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("SHOW TABLES"))
display(notebookutils.fs.ls("Files/raw"))

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
