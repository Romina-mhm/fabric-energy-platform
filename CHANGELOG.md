# Changelog

All notable changes to this project are documented here.

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
