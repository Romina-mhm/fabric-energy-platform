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

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_load (
    area_code           STRING    NOT NULL,
    ts_utc              TIMESTAMP NOT NULL COMMENT 'Start of the interval, UTC',
    resolution_minutes  INT       NOT NULL COMMENT '15 or 60',
    load_mw             DOUBLE    COMMENT 'Average power during the interval',
    energy_mwh          DOUBLE    COMMENT 'load_mw * resolution_minutes / 60',
    _source_file        STRING,
    _bronze_ingested_at TIMESTAMP,
    _silver_updated_at  TIMESTAMP
) USING DELTA
COMMENT 'Actual total load per area and interval (ENTSO-E)'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_generation (
    area_code           STRING    NOT NULL,
    ts_utc              TIMESTAMP NOT NULL,
    resolution_minutes  INT       NOT NULL,
    psr_type_code       STRING    NOT NULL COMMENT 'ENTSO-E production type, e.g. B16 = Solar',
    direction           STRING    NOT NULL COMMENT 'generation | consumption (e.g. pumped storage pumping)',
    power_mw            DOUBLE,
    energy_mwh          DOUBLE,
    co2e_kg             DOUBLE    COMMENT 'energy_mwh * lifecycle gCO2e/kWh (IPCC AR5); NULL when no factor',
    _source_file        STRING,
    _bronze_ingested_at TIMESTAMP,
    _silver_updated_at  TIMESTAMP
) USING DELTA
COMMENT 'Actual generation per production type, area and interval (ENTSO-E)'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_price (
    area_code           STRING    NOT NULL COMMENT 'Bidding zone, e.g. IT_NORD, DE_LU',
    ts_utc              TIMESTAMP NOT NULL,
    resolution_minutes  INT       NOT NULL COMMENT '60 before Oct 2025, 15 after',
    price_eur_mwh       DOUBLE,
    _source_file        STRING,
    _bronze_ingested_at TIMESTAMP,
    _silver_updated_at  TIMESTAMP
) USING DELTA
COMMENT 'Day-ahead prices per bidding zone and interval (ENTSO-E)'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_flow (
    from_area_code      STRING    NOT NULL,
    to_area_code        STRING    NOT NULL,
    ts_utc              TIMESTAMP NOT NULL,
    resolution_minutes  INT       NOT NULL,
    flow_mw             DOUBLE,
    energy_mwh          DOUBLE,
    _source_file        STRING,
    _bronze_ingested_at TIMESTAMP,
    _silver_updated_at  TIMESTAMP
) USING DELTA
COMMENT 'Physical cross-border flows, directed (ENTSO-E)'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")
print("energy tables ready")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_weather_hourly (
    location_code        STRING    NOT NULL,
    country_code         STRING    NOT NULL,
    ts_utc               TIMESTAMP NOT NULL,
    temperature_2m       DOUBLE    COMMENT 'degC',
    wind_speed_10m       DOUBLE    COMMENT 'km/h',
    wind_speed_100m      DOUBLE    COMMENT 'km/h, relevant for wind turbines',
    shortwave_radiation  DOUBLE    COMMENT 'W/m2, relevant for solar',
    cloud_cover          DOUBLE    COMMENT '%',
    precipitation        DOUBLE    COMMENT 'mm',
    data_source          STRING    NOT NULL COMMENT 'archive (reanalysis) | forecast',
    _source_file         STRING,
    _bronze_ingested_at  TIMESTAMP,
    _silver_updated_at   TIMESTAMP
) USING DELTA
COMMENT 'Hourly weather per location; archive values replace forecast values'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_weather_country_hourly (
    country_code         STRING    NOT NULL,
    ts_utc               TIMESTAMP NOT NULL,
    temperature_2m       DOUBLE,
    wind_speed_100m      DOUBLE,
    shortwave_radiation  DOUBLE,
    cloud_cover          DOUBLE,
    precipitation        DOUBLE,
    archive_share        DOUBLE    COMMENT 'Share of the weight coming from archive data (1.0 = fully final)',
    _silver_updated_at   TIMESTAMP
) USING DELTA
COMMENT 'Hourly weather per country: weighted average of its locations'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_holiday (
    country_code        STRING NOT NULL,
    holiday_date        DATE   NOT NULL,
    local_name          STRING,
    name                STRING,
    is_nationwide       BOOLEAN,
    _source_file        STRING,
    _silver_updated_at  TIMESTAMP
) USING DELTA
COMMENT 'Public holidays per country (Nager.Date)'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS dq_quarantine (
    table_name     STRING,
    record_key     STRING    COMMENT 'Readable key of the rejected row',
    rule_name      STRING    COMMENT 'Which data-quality rule failed',
    details        STRING,
    source_file    STRING,
    detected_at    TIMESTAMP
) USING DELTA
COMMENT 'Rows rejected by data-quality rules (kept for review, not silently dropped)'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS ctl_silver_state (
    dataset                    STRING,
    processed_until_ingested_at TIMESTAMP COMMENT 'Bronze files ingested up to this time are processed',
    updated_at_utc             TIMESTAMP
) USING DELTA
COMMENT 'Silver incremental state: which Bronze loads have been processed'
""")
print("weather, holiday, quality and state tables ready")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SHOW TABLES").show(truncate=False)

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
