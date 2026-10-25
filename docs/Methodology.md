---
title: "European Energy Platform — Methodology and Data Definitions"
subtitle: "How the KPIs are calculated, where the data comes from, and what the numbers can and cannot tell you"
date: "Version 1.1 — October 2026"
---

# 1. Purpose and scope

This document explains how the European Energy Platform calculates its key indicators. It is the reference for the Power BI report, the Fabric data agent and the Foundry agent.

- **Countries:** Italy (IT), France (FR), Germany (DE), Netherlands (NL).
- **Period:** from 1 January 2024 to the latest loaded hour (the *Data Through Date*).
- **Time zone:** all timestamps are in **UTC**. Daily and monthly values are UTC days and months.
- **Grain:** hourly. Data published at 15-minute resolution is aggregated to the hour.

# 2. Data sources

| Source | What it provides | How it is loaded |
|---|---|---|
| ENTSO-E Transparency Platform (API) | Actual load, actual generation per production type, day-ahead prices, cross-border physical flows | PySpark notebooks into the Bronze lakehouse, incremental with watermarks |
| Open-Meteo (API) | Hourly weather (temperature, wind at 100 m, solar radiation), observed archive and recent forecast | PySpark notebook |
| Nager.Date (API) | Public holidays | Pipeline REST copy |
| Azure SQL reference database | Countries, bidding zones, interconnectors, production types and emission factors, weather points | Fabric mirroring, OneLake shortcuts |
| ADLS Gen2 | National renewable targets (KPI thresholds) | Dataflow Gen2 |

# 3. Carbon intensity (gCO2e/kWh)

**Definition.** Grams of CO2-equivalent emitted per kWh of electricity **generated in the country**.

**Formula.**

> Carbon intensity = total CO2e (kg) ÷ generation with a known emission factor (MWh)

For each hour and production type: CO2e (kg) = generation (MWh) × emission factor (gCO2e/kWh). MWh × gCO2e/kWh gives kg CO2e.

**Calculated over the whole requested scope.** The ratio is always computed as a sum divided by a sum over the full scope (countries and period). It is never an average of hourly or daily percentages, because averaging ratios gives a wrong result when volumes differ between hours.

## 3.1 Emission factors

The factors are **lifecycle** emissions (construction, fuel supply, operation and decommissioning), taken from the **IPCC Fifth Assessment Report (AR5, 2014), Working Group III, Annex III, median values**. Because they are lifecycle values, wind, solar and nuclear have small non-zero factors.

| ENTSO-E code | Production type | gCO2e/kWh | Note |
|---|---|---|---|
| B01 | Biomass | 230 | IPCC AR5 median, dedicated biomass |
| B02 | Brown coal / lignite | 820 | Proxy: coal. Lignite is probably higher, so this is conservative |
| B03 | Coal-derived gas | 820 | Proxy: coal |
| B04 | Fossil gas | 490 | IPCC AR5 median, gas combined cycle |
| B05 | Hard coal | 820 | IPCC AR5 median, pulverised coal |
| B09 | Geothermal | 38 | IPCC AR5 median |
| B11 / B12 | Hydro (run-of-river, reservoir) | 24 | IPCC AR5 median |
| B13 | Marine | 17 | IPCC AR5 median |
| B14 | Nuclear | 12 | IPCC AR5 median |
| B16 | Solar | 48 | IPCC AR5 median, utility PV |
| B18 | Wind offshore | 12 | IPCC AR5 median |
| B19 | Wind onshore | 11 | IPCC AR5 median |
| B06, B07, B08 | Oil, oil shale, peat | none | No IPCC AR5 value |
| B15 | Other renewable | none | No single IPCC AR5 value |
| B17, B20 | Waste, Other | none | No IPCC AR5 value |
| B10, B25 | Pumped storage, energy storage | none | Excluded on purpose: storage shifts energy, it does not generate it |

Factors are stored as a **slowly changing dimension (SCD Type 2)**. If a factor changes, the old value is kept with its validity period, so historical CO2e is always calculated with the factor that was valid at that hour.

# 4. Factor coverage (%)

**Definition.** The share of generation (MWh) that has a known emission factor.

> Factor coverage = generation with a known emission factor (MWh) ÷ total generation (MWh)

**Why it exists.** Some production types have no defensible emission factor (see the table above). Their generation is **left out of both parts** of the carbon intensity ratio. It is **not** treated as zero-carbon, because that would make a country look cleaner than the data can prove.

**How to read it.**

- **90% or more:** the carbon intensity is reliable.
- **Below 90%:** the carbon intensity describes the known part of the mix only and should be read with care. The true value for the whole country lies between a lower bound (carbon intensity × coverage, if all unknown generation were zero-carbon) and a value above the reported one (if the unknown generation were mostly fossil).

**Example.** The Netherlands has a much lower factor coverage than the other countries, because a large share of Dutch generation is reported under production types without an emission factor. Its carbon intensity therefore describes only part of Dutch generation, and the true national value may be lower or higher. For the current coverage and carbon intensity, use the live data (Factor Coverage % and Carbon Intensity measures), not this document.

# 5. Other generation KPIs

- **Renewable % =** renewable generation ÷ total generation. Renewable: biomass, geothermal, hydro (run-of-river and reservoir), marine, other renewable, solar, wind.
- **Low-carbon % =** low-carbon generation ÷ total generation. Low-carbon = renewable **plus nuclear**.
- **Fuel categories:** Fossil, Nuclear, Renewable, Storage, Other. Pumped storage and batteries are classed as Storage and are neither renewable nor fossil.
- **Renewable gap vs target (percentage points) =** (renewable % − national target %) × 100. A negative value means below target.

# 6. Data-handling rules

**Generation direction.** ENTSO-E publishes some series as consumption (for example pumped-storage pumping and plants' own use). The Netherlands reports a consumption series for most production types. Every series carries a direction, and **only generation is counted** in generation and carbon KPIs.

**Mixed resolutions.** Sources switched from 60-minute to 15-minute data at different dates (for example day-ahead prices from 1 October 2025, and French and Italian load from 31 December 2024). Some Dutch borders publish flows at both resolutions at the same time. Resolution is part of the record key, energy is summed per hour, and flows use one resolution per border-hour so nothing is counted twice. MW values are never averaged across mixed resolutions.

**Omitted repeated values.** For curve type A03, ENTSO-E omits a point when the value repeats. These points are forward-filled.

**Prices.** Day-ahead prices belong to **bidding zones**, not countries (Germany shares the DE-LU zone with Luxembourg; Italy has seven zones). The hourly price is the official hourly auction price where it exists, otherwise the average of the four 15-minute prices. The country average price is a simple average of its zones' hourly prices (not weighted by volume).

**Weather.** Observed (archive) values always take priority over forecasts. A forecast never overwrites an observation. Country values are weighted averages of several locations.

**Late corrections.** Data is loaded incrementally and merged, so corrections published later by ENTSO-E replace earlier values without duplicating them.

**Data quality.** Each run checks uniqueness, referential integrity, valid ranges, completeness and freshness. Records that cannot be parsed go to a quarantine table instead of the reports. Gold is only loaded if Silver and Gold reconcile.

# 7. What the data cannot answer

- **Consumption-based carbon.** Carbon intensity is based on **domestic generation**. Imported electricity is not included. A country that imports a lot of low-carbon power (for example Italy from France) consumes cleaner electricity than its generation-based value shows.
- **Forecasts.** The platform contains historical data only, up to the Data Through Date. Questions about tomorrow or future periods cannot be answered.
- **Retail or household prices.** Only wholesale day-ahead prices are available. Household prices include network charges, taxes and retail margins that are not in the data.
- **Other countries.** Only IT, FR, DE and NL are covered.
- **Plant-level detail.** Generation is reported per production type, not per power plant.
