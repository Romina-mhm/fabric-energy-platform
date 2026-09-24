# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "warehouse": {
# META       "default_warehouse": "dc27c302-9c49-42f0-a416-a3465732bbf7",
# META       "known_warehouses": [
# META         {
# META           "id": "dc27c302-9c49-42f0-a416-a3465732bbf7",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%configure -f
# MAGIC {
# MAGIC     "defaultLakehouse": {
# MAGIC         "name": "lh_bronze"
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Smoke test: proves every Day-1 connection works before building Bronze.
KEY_VAULT_URI = "https://energyanalytics-kv.vault.azure.net/"   # check on the vault's Overview page
SECRET_NAME   = "entsoe-api-key"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

api_key = notebookutils.credentials.getSecret(KEY_VAULT_URI, SECRET_NAME)

# Never print a secret. Only prove we got something.
print(f"Secret retrieved: {len(api_key)} characters")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from importlib.metadata import version
print("entsoe-py version:", version("entsoe-py"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import re
import pandas as pd
from entsoe import EntsoeRawClient

raw_client = EntsoeRawClient(api_key=api_key)

end = pd.Timestamp.now(tz="Europe/Amsterdam").normalize()   # today 00:00 local time
start = end - pd.Timedelta(days=1)                           # yesterday 00:00

xml_text = raw_client.query_load("NL", start=start, end=end)

print("XML size (characters):", len(xml_text))
print("Resolution(s):", set(re.findall(r"<resolution>(.*?)</resolution>", xml_text)))
print("Data points:", xml_text.count("<Point>"))
print(xml_text[:600])   # first lines of the raw document

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

resp = requests.get(
    "https://api.open-meteo.com/v1/forecast",
    params={
        "latitude": 52.3676, "longitude": 4.9041,
        "hourly": "temperature_2m,wind_speed_100m,shortwave_radiation,cloud_cover",
        "past_days": 1, "forecast_days": 1, "timezone": "UTC",
    },
    timeout=30,
)
resp.raise_for_status()
weather = pd.DataFrame(resp.json()["hourly"])
print(f"Weather rows: {len(weather)}")
display(weather.head())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

resp = requests.get("https://date.nager.at/api/v3/PublicHolidays/2026/NL", timeout=30)
resp.raise_for_status()
holidays = pd.DataFrame(resp.json())[["date", "localName", "name"]]
display(holidays)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("""
    SELECT c.country_code, c.country_name, c.is_active, COUNT(a.area_code) AS areas
    FROM country c
    LEFT JOIN area a ON a.country_code = c.country_code
    GROUP BY c.country_code, c.country_name, c.is_active
    ORDER BY c.is_active DESC, c.country_code
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

kpi = spark.read.option("header", True).csv("Files/business/kpi_thresholds.csv")
display(kpi)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
