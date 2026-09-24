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
