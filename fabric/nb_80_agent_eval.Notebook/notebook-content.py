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

%pip install -U fabric-data-agent-sdk -q

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import sempy.fabric as fabric
from fabric.dataagent.evaluation import evaluate_data_agent, get_evaluation_summary, get_evaluation_details

MODEL = "sm_energy_sustainability"
AGENT = "da_energy_sustainability"

def dax(q):
    return fabric.evaluate_dax(dataset=MODEL, dax_string=q)

def one(q):
    return dax(q).iloc[0, 0]

DE = 'dim_country[country_code]="DE"'
FR = 'dim_country[country_code]="FR"'
IT = 'dim_country[country_code]="IT"'
NL = 'dim_country[country_code]="NL"'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tune = []
def addt(q, a): tune.append([q, a])

r = dax(f'''EVALUATE TOPN(1, CALCULATETABLE(ADDCOLUMNS(SUMMARIZE(dim_hour, dim_hour[hour_key]),
        "CI",[Carbon Intensity gCO2e/kWh]), {DE}), [CI], ASC)''')
addt("When is electricity greenest in Germany?",
     f"Typically {int(r.iloc[0,0]):02d}:00 UTC, about {r.iloc[0,1]:.0f} gCO2e/kWh")

v = one(f'EVALUATE ROW("v", CALCULATE([Carbon Intensity gCO2e/kWh], {NL}, dim_date[date]=DATE(2025,6,15)))')
addt("What was the carbon intensity in the Netherlands on 15 June 2025?", f"{v:.1f} gCO2e/kWh (daily value)")

r = dax('EVALUATE ROW("ci",[Carbon Intensity gCO2e/kWh],"cov",[Factor Coverage %])')
addt("What is the overall carbon intensity across all countries?",
     f"{r.iloc[0,0]:.1f} gCO2e/kWh, coverage {r.iloc[0,1]:.1%}, period 1 Jan 2024 to latest data")

r = dax('''EVALUATE TOPN(1, FILTER(ADDCOLUMNS(VALUES(dim_country[country_name]),"CI",[Carbon Intensity gCO2e/kWh]),
        NOT ISBLANK([CI])), [CI], ASC)''')
addt("Which country has the lowest carbon intensity?", f"{r.iloc[0,0]} ({r.iloc[0,1]:.1f} gCO2e/kWh)")

v = one(f'EVALUATE ROW("v", CALCULATE([Renewable %], {NL}))')
addt("How far is the Netherlands from its renewable target?",
     f"Renewable share {v:.1%} vs 70% 2030 target, about {70 - v*100:.0f} percentage points below")

addt("What's the carbon intensity in Spain?", "Declines: Spain is not in the data")

tune_df = pd.DataFrame(tune, columns=["question", "expected_answer"])
display(tune_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gt = []
def add(q, a): gt.append([q, a])

h = one(f'EVALUATE ROW("h", CALCULATE([Greenest Hour], {FR}))')
add("What's the best time of day to charge an EV in France to minimise emissions?", f"Around {int(h):02d}:00 UTC")

r = dax(f'''EVALUATE TOPN(1, CALCULATETABLE(ADDCOLUMNS(SUMMARIZE(dim_date, dim_date[month], dim_date[month_name]),
        "CI",[Carbon Intensity gCO2e/kWh]), {DE}, dim_date[year]=2025), [CI], DESC)''')
add("Which month had the highest carbon intensity in Germany in 2025?", f"{r.iloc[0,1]} ({r.iloc[0,2]:.1f} gCO2e/kWh)")

v = one(f'EVALUATE ROW("v", CALCULATE([Renewable %], {IT}, dim_date[year]=2025))')
add("What was Italy's renewable share in 2025?", f"{v:.1%}")

t = dax(f'''EVALUATE CALCULATETABLE(ADDCOLUMNS(VALUES(dim_date[year]),"CI",[Carbon Intensity gCO2e/kWh]),
        {DE}, dim_date[year] IN {{2024,2025}}) ORDER BY dim_date[year]''')
a, b = t.iloc[0,1], t.iloc[1,1]
add("How did Germany's carbon intensity change from 2024 to 2025?",
    f"{'Decreased' if b < a else 'Increased'} from {a:.1f} to {b:.1f} gCO2e/kWh ({(b-a)/a:+.1%})")

v = one(f'EVALUATE ROW("v", CALCULATE([Avg Price EUR/MWh], {DE}, dim_date[year]=2025))')
add("What was the average day-ahead price in Germany in 2025?", f"EUR {v:.1f}/MWh")

v = one(f'EVALUATE ROW("v", CALCULATE([Negative Price Hours], {NL}, dim_date[year]=2025))')
add("How many negative-price hours did the Netherlands have in 2025?", f"{int(v)} hours")

v = one(f'EVALUATE ROW("v", CALCULATE([Net Imports MWh], {FR}))')
add("Is France a net importer or exporter of electricity?",
    ("Net importer" if v > 0 else "Net exporter") + f" ({abs(v)/1e6:.1f} TWh net over the period)")

v = one(f'''EVALUATE ROW("v", DIVIDE(CALCULATE([Generation MWh], {NL}, dim_production_type[fuel_category]="Fossil"),
        CALCULATE([Generation MWh], {NL})))''')
add("What share of Dutch electricity generation comes from fossil fuels?", f"{v:.1%}")

r = dax('''EVALUATE TOPN(1, FILTER(ADDCOLUMNS(VALUES(dim_country[country_name]),"R",[Renewable %]),
        NOT ISBLANK([R])), [R], DESC)''')
add("Which country has the highest renewable share?", f"{r.iloc[0,0]} ({r.iloc[0,1]:.1%})")

r = dax(f'''EVALUATE TOPN(1, CALCULATETABLE(ADDCOLUMNS(SUMMARIZE(dim_hour, dim_hour[hour_key]),
        "P",[Avg Price EUR/MWh]), {IT}), [P], ASC)''')
add("What is the cheapest hour of the day for electricity in Italy?",
    f"Around {int(r.iloc[0,0]):02d}:00 UTC (average across Italian bidding zones, EUR {r.iloc[0,1]:.1f}/MWh)")

r = dax('EVALUATE ROW("ci",[Carbon Intensity gCO2e/kWh],"cov",[Factor Coverage %])')
add("How clean is the electricity?",
    f"{r.iloc[0,0]:.1f} gCO2e/kWh for IT, FR, DE, NL over the full period (coverage {r.iloc[0,1]:.1%}); assumption stated")

add("What will the carbon intensity be in Germany tomorrow?", "Declines: no forecasts in the data")
add("How much does a household in Italy pay per kWh?", "Declines: only wholesale day-ahead prices are available")

v = one(f'EVALUATE ROW("v", CALCULATE([Avg Wind 100m km/h], {DE}, dim_date[season]="Winter"))')
add("What was the average wind speed in Germany in winter?", f"{v:.1f} km/h at 100 m")

eval_df = pd.DataFrame(gt, columns=["question", "expected_answer"])
display(eval_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

critic_prompt = """
You are a strict evaluator. Compare the data agent's most recent answer in this conversation (the answer immediately above) with the expected answer.

Question: {query}

Expected answer:
{expected_answer}

Rules:
- Numbers must match the expected value within +/-1% (rounding differences are fine).
- The answer must cover the same scope (same country, period and metric) as the expected answer.
- If the expected answer starts with "Declines", the agent's answer is correct only if it says the data is not available and gives NO invented number.
- Extra context or caveats are fine, as long as they do not contradict the expected answer.
- The same value in an equivalent unit (e.g. m/s vs km/h, MWh vs TWh) counts as a match after conversion.

Is the agent's answer equivalent to the expected answer? Respond with 'yes' or 'no' only.
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

run_tune = evaluate_data_agent(tune_df, AGENT, table_name="agent_eval_tuning", critic_prompt=critic_prompt)
run_held = evaluate_data_agent(eval_df, AGENT, table_name="agent_eval_heldout", critic_prompt=critic_prompt)
print("tuning run:", run_tune, "| held-out run:", run_held)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("TUNING"); display(get_evaluation_summary("agent_eval_tuning"))
print("HELD-OUT"); display(get_evaluation_summary("agent_eval_heldout"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary_held = get_evaluation_summary("agent_eval_heldout")
run_held = summary_held["evaluation_id"].iloc[-1]   # latest held-out run
print("Held-out run:", run_held)

get_evaluation_details(run_held, "agent_eval_heldout", get_all_rows=True, verbose=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
