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

AGENT_NAME      = "da_energy_sustainability"   # published Fabric data agent
SEMANTIC_MODEL  = "sm_energy_sustainability"   # model used for live ground truth
QUESTIONS_FILE  = "Files/agent_eval/questions.json"
SETS            = ["tuning", "heldout"]         # which question sets to run
N_RUNS          = 3                             # repeat for stability (e.g. 3 for a frozen release)
MAX_QUESTIONS   = None                          # e.g. 3 for a cheap smoke test; None = all
TABLE_PREFIX    = "agent_eval"                  # results tables: agent_eval_tuning, agent_eval_heldout
CRITIC_PROMPT   = """
You are a strict evaluator. Compare the data agent's most recent answer in this conversation (the answer immediately above) with the expected answer.

Question: {query}

Expected answer:
{expected_answer}

Rules:
- Numbers must match the expected value within +/-1% (rounding differences are fine).
- The same value in an equivalent unit (e.g. m/s vs km/h, MWh vs TWh, fraction vs percentage) counts as a match after conversion.
- The answer must cover the same scope (same country, period and metric) as the expected answer.
- If the expected answer starts with "Declines", the answer is correct if it clearly says the requested quantity is not in the data and does not present any number as that quantity. Offering clearly labelled related data is fine.
- Extra context or caveats are fine, as long as they do not contradict the expected answer.

Is the agent's answer equivalent to the expected answer? Respond with 'yes' or 'no' only.
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import pandas as pd
import sempy.fabric as fabric
from fabric.dataagent.evaluation import (
    evaluate_data_agent, get_evaluation_summary, get_evaluation_details)

def run_dax(query: str) -> pd.DataFrame:
    return fabric.evaluate_dax(dataset=SEMANTIC_MODEL, dax_string=query)

def expected_answer(q: dict) -> str:
    """Live ground truth: run the DAX and fill the answer template.
    Questions without DAX (e.g. refusals) keep their literal expected answer."""
    dax = (q.get("truth_dax") or "").strip()
    if not dax:
        return q["expected_answer"]
    row = run_dax(dax).iloc[0]
    values = {str(k).split("[")[-1].strip("[]"): v for k, v in row.items()}
    return q["expected_answer"].format(**values)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

with open(f"/lakehouse/default/{QUESTIONS_FILE}") as f:
    questions = json.load(f)

required = {"id", "set", "question", "expected_answer"}
missing = [q.get("id") for q in questions if not required.issubset(q)]
assert not missing, f"Questions missing fields: {missing}"

qdf = pd.DataFrame(questions)
qdf = qdf[qdf["set"].isin(SETS)].reset_index(drop=True)
qdf["expected_answer"] = [expected_answer(q) for q in qdf.to_dict("records")]
if MAX_QUESTIONS:
    qdf = qdf.groupby("set").head(MAX_QUESTIONS).reset_index(drop=True)

display(qdf[["id", "set", "question", "expected_answer"]])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

runs = []
for run_no in range(1, N_RUNS + 1):
    for s in SETS:
        df = qdf[qdf["set"] == s][["question", "expected_answer"]].reset_index(drop=True)
        if df.empty:
            continue
        table = f"{TABLE_PREFIX}_{s}"
        eval_id = evaluate_data_agent(df, AGENT_NAME, table_name=table,
                                      critic_prompt=CRITIC_PROMPT)
        runs.append({"run": run_no, "set": s, "table": table,
                     "evaluation_id": eval_id, "questions": len(df)})

runs_df = pd.DataFrame(runs)
display(runs_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rows = []
for r in runs_df.itertuples():
    s = get_evaluation_summary(r.table)
    s = s[s["evaluation_id"] == r.evaluation_id].iloc[0]
    rows.append({"run": r.run, "set": r.set, "evaluation_id": r.evaluation_id,
                 "correct": int(s["T"]), "wrong": int(s["F"]),
                 "unclear": int(s["?"]), "pct": float(s["%"])})
per_run = pd.DataFrame(rows)
display(per_run)

overall = (per_run.groupby("set")
           .agg(correct=("correct", "sum"), wrong=("wrong", "sum"),
                min_pct=("pct", "min"), max_pct=("pct", "max")))
overall["accuracy_pct"] = (100 * overall["correct"]
                           / (overall["correct"] + overall["wrong"])).round(1)
display(overall.reset_index())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fails = []
for r in runs_df.itertuples():
    d = get_evaluation_details(r.evaluation_id, r.table, get_all_rows=True, verbose=False)
    is_true = d.astype(str).apply(lambda c: c.str.lower().eq("true")).any(axis=1)
    fails.append(d[~is_true].assign(run=r.run, set=r.set))
fails_df = pd.concat(fails, ignore_index=True) if fails else pd.DataFrame()
print(f"{len(fails_df)} failed or unclear answers")
display(fails_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for t in ["agent_eval_tuning", "agent_eval_heldout"]:
    print(t)
    display(get_evaluation_summary(t))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
