# Changelog

All notable changes to this project are documented here.

## [0.6.0] - 2026-10-03 — Performance, Dataflow Gen2 & SCD Type 2
### Added
- Dataflow Gen2 `df_kpi_thresholds`: KPI targets CSV (ADLS) → `lh_silver.silver_kpi_threshold` → `dim.dim_kpi_threshold`; runs in the monthly branch of `pl_master`
- SCD Type 2 dimension `dim.dim_emission_factor` (`valid_from`, `valid_to`, `is_current`) with `dim.usp_scd2_emission_factor`; generation CO2e now computed in Gold with the factor valid at each hour
- Shared Silver helper notebook `nb_lib_silver` (`align`, `merge_into` with Delta version check) used via `%run`
### Changed
- Open-Meteo requests batched for all locations per time window (22 → 2 API calls), one raw file per location kept
- Data-quality notebook: incremental daily checks (last 7 days) with a weekly full scan; `OPTIMIZE` only above a file-count threshold
- Broadcast join for small lookups in the Silver ENTSO-E notebook; test clutter removed from notebooks
- Dimension refresh switched to `TRUNCATE` + `INSERT`
- Capacity back to F2 after the Azure credit ended (paused when idle)
### Fixed
- `pl_master` reported success even when a step failed (a deactivated alert counts as handled) → **Fail activity** at the end of the failure path
### Performance
- Daily end-to-end run reduced from ~27 to ~17 minutes

## [0.5.0] - 2026-09-28 — Security, CI/CD & real-time
### Added
- Row-level security on `wh_gold` by country (`sec.user_country`, `sec.fn_country_filter`, `sec.country_policy`)
- Column-level `DENY` on `fact_price_hourly.max_price_eur_mwh`; dynamic data masking on `ops.gold_run_log.message`
- OneLake data access roles on the lakehouse; least-privilege item sharing
- Deployment pipeline dev → test
- Eventstream sample → Eventhouse with KQL window queries
- Activator alert on pipeline job failure

## [0.4.0] - 2026-09-28 — Gold layer & orchestration
### Added
- `wh_gold` Fabric Warehouse with star schema (schemas `dim`, `fact`, `ops`)
- Dimensions via CTAS: date, hour, country, area, production type (with emission factors), holiday
- Hourly facts: load, generation by fuel, carbon intensity (renewable %, low-carbon %, gCO2e/kWh, factor coverage %), day-ahead price, cross-border flow, weather
- Incremental delete-insert stored procedures per fact (default window: last 7 days, transactional)
- `ops.usp_load_gold`: one entry point (dims → facts) with a Silver-vs-Gold reconciliation gate that fails the run on mismatch; results logged to `ops.gold_run_log`
- `pl_master` pipeline: Bronze ENTSO-E → Bronze weather → monthly holidays (If Condition) → Silver → Gold; daily schedule 06:00 Europe/Rome
- Failure-alert path (On fail + On skip from last step), currently deactivated
### Business rules
- Hourly price = official hourly auction price where it exists, else average of 15-min prices (handles DE_LU dual publication)
- Energy = SUM(MWh) per hour; flows use one resolution per border-hour to avoid double counting
- Carbon intensity reported with factor coverage % instead of treating unclassified generation as zero-carbon
### Known limitations
- ENTSO-E under-reports Dutch rooftop solar and classifies ~33% of NL generation as "Other" → NL renewable share is understated; shown with coverage %

## [0.3.0] - 2026-09-26 — Silver layer
### Added
- ENTSO-E XML parser (positions → UTC timestamps, A03 forward-fill, generation vs consumption) running distributed on Spark executors
- Silver Delta tables: load, generation (with MWh and lifecycle CO2e), day-ahead price, cross-border flow, weather (location + weighted country-hour), public holidays
- Idempotent loads: dedupe by business key (latest ingest wins) + conditional MERGE; Silver state table for incremental processing
- Weather MERGE guard: observed archive values are never overwritten by forecasts
- Holidays as full-snapshot MERGE incl. delete of vanished rows
- Data-quality notebook: uniqueness, referential integrity, value ranges, completeness, freshness → `dq_check_result`
- OPTIMIZE with V-Order on all Silver tables
- `pl_silver` pipeline (4 notebooks, shared Spark session via high concurrency)
### Fixed
- MERGE metrics now reported only when a new Delta version is committed
- UTF-8 BOM in Copy-activity JSON files
### Data findings
- DQ detected the 28 Apr 2025 Iberian blackout in ES↔FR flows
- DE_LU publishes both 15-min and hourly day-ahead prices (639 overlap days) → explicit Gold rule

## [0.2.0] - 2026-09-25 — Bronze
### Added
- Bronze audit log (`ctl_ingest_log`) and incremental-load watermarks (`ctl_watermark`)
- ENTSO-E ingestion notebook: consumption, generation by type, day-ahead prices (10 bidding zones incl. 7 Italian), cross-border flows (30 directions); raw XML kept unchanged; monthly chunks, parallel calls, watermark + 3-day re-pull
- Backfill 2024-01-01 → today: 1,584 chunks, 0 errors; resumable from the audit log
- Weather ingestion (Open-Meteo): reanalysis archive + recent forecast for 11 locations, raw JSON
- Holidays pipeline: Lookup → Filter → ForEach → REST Copy with dynamic expressions
- Pipeline `pl_bronze_entsoe` running the ingestion notebook as a background job
### Changed
- Capacity scaled F2 → F4 (Spark 430 limit)
### Fixed
- Pipeline-safe notebooks (no pandas `display()`), notebook exit values for pipelines
- HTTP timeouts and retries with back-off for external APIs

## [0.1.0] - 2026-09-24 — Foundation
### Added
- Azure SQL reference database (free offer): 12 countries, 20 ENTSO-E market areas, dataset-to-area mapping, 40 directed interconnectors, 21 generation types with IPCC AR5 lifecycle emission factors, 23 weather points; 4-country pilot (IT, FR, DE, NL)
- Least-privilege login for Fabric Mirroring
- Fabric workspaces `energy-dev` and `energy-test` on an F2 capacity, grouped in the `Energy` domain
- Fabric Git integration: `energy-dev` synced to `main` (`fabric/` folder)
- Lakehouses `lh_bronze` and `lh_silver`
- Spark environment `env_energy` with a pinned `entsoe-py`
- Mirrored Azure SQL database `mirror_energy_ref`, exposed in `lh_bronze` through OneLake table shortcuts
- OneLake shortcut to ADLS Gen2 business landing zone (KPI thresholds)
- Secrets in Azure Key Vault, read at run time with `notebookutils`
- Smoke-test notebook covering Key Vault, ENTSO-E, Open-Meteo, holidays API, mirroring and shortcuts
