# Incidents and lessons learned

| # | Date | Problem | Root cause | Fix | Lesson |
|---|---|---|---|---|---|
| 1 | 2026-09-24 | entsoe-py pandas client raised KeyError on a timestamp | Library timezone handling | Switched to raw XML client; raw data kept in Bronze | Keep raw source data so parsing can change without re-ingesting |
| 2 | 2026-09-25 | Spark jobs refused with HTTP 430 | F2 capacity too small for the default Spark session | Scaled to F4; stop idle sessions | Size capacity to the workload; monitor concurrency |
| 3 | 2026-09-25 | Backfill stopped at 1,250 / 1,584 chunks when the laptop disconnected | Interactive session tied to the browser | Resumable backfill from the audit log; run via pipeline | Long loads belong in pipelines; make loads resumable and idempotent |
| 4 | 2026-09-25 | Notebook failed in pipeline with KeyError 'data' | `display()` of pandas objects in a non-interactive run | `print()` + `notebookutils.notebook.exit()` | Write notebooks to run unattended |
| 5 | 2026-09-25 | Weather API ReadTimeouts on yearly requests | Heavy requests exceeded a 60 s timeout | 240 s timeout, retries with back-off | External APIs need timeouts, retries and idempotent reruns |
| 6 | 2026-09-25 | REST call sent literal expression text | Leading spaces before `@` in a pipeline expression | Expression starts with `@` | Pipeline expressions are only evaluated when the value starts with `@` |
