/* =====================================================================
   02_ref_schema_and_seed.sql   (v2.1 — 12 countries + prices + weather + emission factors)
   RUN AGAINST:  sqldb-energy-ref   (the user database, NOT master)

   What this database is:
   The reference / master-data system for the Fabric energy platform.
   Slowly changing data a business user would maintain: which countries
   and market areas we track, which area code to use for which dataset,
   borders, production-type categories, weather locations, and which
   user may see which country (for RLS). Fabric Mirroring replicates it
   into OneLake; nothing in Fabric writes back to it.

   Key modelling idea — COUNTRY is not the same as AREA:
   ENTSO-E publishes data per "area" (EIC code). Usually one country =
   one area, but not always:
     - Germany: load/generation use the DE country area, but day-ahead
       PRICES are published for the DE-LU bidding zone (shared with
       Luxembourg).
     - Italy: one country area for load/generation, but SEVEN bidding
       zones for prices (North, Centre-North, Centre-South, South,
       Sicily, Sardinia, Calabria).
   So: ref.country (business view) -> ref.area (market/technical view)
   -> ref.dataset_area (which area to query for which dataset).

   What does NOT live here (on purpose):
   - Measurements (load, generation, prices, flows, weather): API -> Bronze.
   - Public holidays: pulled from a public API by a Fabric pipeline.
   - Ingestion watermarks: pipeline state, owned by Fabric. A mirrored
     DB is read-only in Fabric, so a watermark here would force the
     pipeline to write back to the source system.

   Idempotent: safe to run as many times as you like.
   Business-owned switches (is_active, is_enabled) are set on first
   insert only — re-running never overwrites a user's change.
   Timestamps are datetime2(0): mirroring can't use datetime2(7) in PKs.
   ===================================================================== */

SET XACT_ABORT ON;
GO

/* ---------- ONLY if you already ran the OLD v1 script ----------------
   v1 created tables with different columns (ref.ingest_scope,
   country-based ref.interconnector). This DB holds only seed data, so
   the clean fix is to drop them and let v2 recreate everything.
   Uncomment, run once, then comment it out again.
   --------------------------------------------------------------------- */
-- DROP TABLE IF EXISTS sec.user_country, ref.ingest_scope, ref.interconnector,
--                      ref.dataset_area, ref.weather_location, ref.area,
--                      ref.production_type, ref.country;
-- GO

/* ---------- Schemas ------------------------------------------------- */
IF SCHEMA_ID(N'ref') IS NULL EXEC (N'CREATE SCHEMA ref AUTHORIZATION dbo;');
IF SCHEMA_ID(N'sec') IS NULL EXEC (N'CREATE SCHEMA sec AUTHORIZATION dbo;');
GO

/* ---------- ref.country ----------------------------------------------
   is_active = the ONE switch that decides whether we ingest a country.
   Start with a 4-country pilot (IT, FR, DE, NL); scale out by flipping the
   flag — no code change. That's the point of config-driven pipelines.
   --------------------------------------------------------------------- */
IF OBJECT_ID(N'ref.country', N'U') IS NULL
CREATE TABLE ref.country (
    country_code    char(2)      NOT NULL,          -- ISO 3166-1 alpha-2
    country_name    nvarchar(50) NOT NULL,
    time_zone       varchar(40)  NOT NULL,          -- IANA name, for local-time reporting / DST
    is_active       bit          NOT NULL CONSTRAINT df_country_active DEFAULT (0),
    modified_at_utc datetime2(0) NOT NULL CONSTRAINT df_country_mod DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT pk_country PRIMARY KEY (country_code),
    CONSTRAINT ck_country_code CHECK (LEN(country_code) = 2)
);
GO

/* ---------- ref.area --------------------------------------------------
   One row per ENTSO-E area we query. area_code is our readable key
   (same names entsoe-py uses); eic_code is what the API needs.
   --------------------------------------------------------------------- */
IF OBJECT_ID(N'ref.area', N'U') IS NULL
CREATE TABLE ref.area (
    area_code       varchar(10)  NOT NULL,          -- e.g. 'IT_NORD', 'DE_LU', 'FR'
    eic_code        varchar(16)  NOT NULL,
    area_name       nvarchar(60) NOT NULL,
    country_code    char(2)      NOT NULL,          -- owning country (DE_LU -> DE)
    is_country_area bit          NOT NULL,          -- used for load / generation / flows
    is_bidding_zone bit          NOT NULL,          -- used for day-ahead prices
    modified_at_utc datetime2(0) NOT NULL CONSTRAINT df_area_mod DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT pk_area PRIMARY KEY (area_code),
    CONSTRAINT uq_area_eic UNIQUE (eic_code),
    CONSTRAINT fk_area_country FOREIGN KEY (country_code) REFERENCES ref.country (country_code),
    CONSTRAINT ck_area_role CHECK (is_country_area = 1 OR is_bidding_zone = 1)
);
GO

/* ---------- ref.dataset_area -----------------------------------------
   Which area to query for which ENTSO-E dataset.
   Effective ingest scope = dataset_area.is_enabled AND country.is_active.
   --------------------------------------------------------------------- */
IF OBJECT_ID(N'ref.dataset_area', N'U') IS NULL
CREATE TABLE ref.dataset_area (
    dataset         varchar(30)  NOT NULL,
    area_code       varchar(10)  NOT NULL,
    is_enabled      bit          NOT NULL CONSTRAINT df_da_enabled DEFAULT (1),
    modified_at_utc datetime2(0) NOT NULL CONSTRAINT df_da_mod DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT pk_dataset_area PRIMARY KEY (dataset, area_code),
    CONSTRAINT fk_da_area FOREIGN KEY (area_code) REFERENCES ref.area (area_code),
    CONSTRAINT ck_da_dataset CHECK (dataset IN
        ('actual_load', 'actual_generation', 'installed_capacity', 'day_ahead_price'))
);
GO

/* ---------- ref.interconnector ----------------------------------------
   Physical borders between country areas, stored as DIRECTED pairs in
   both directions: ENTSO-E flows are directional, and
   net import = flow(in) - flow(out).
   NOTE: Germany's flows may need the DE_LU zone instead of the DE
   country area — we verify this against the API in the Bronze phase
   and fix it here (data, not code) if needed.
   --------------------------------------------------------------------- */
IF OBJECT_ID(N'ref.interconnector', N'U') IS NULL
CREATE TABLE ref.interconnector (
    from_area_code  varchar(10)  NOT NULL,
    to_area_code    varchar(10)  NOT NULL,
    is_enabled      bit          NOT NULL CONSTRAINT df_ic_enabled DEFAULT (1),
    modified_at_utc datetime2(0) NOT NULL CONSTRAINT df_ic_mod DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT pk_interconnector PRIMARY KEY (from_area_code, to_area_code),
    CONSTRAINT fk_ic_from FOREIGN KEY (from_area_code) REFERENCES ref.area (area_code),
    CONSTRAINT fk_ic_to   FOREIGN KEY (to_area_code)   REFERENCES ref.area (area_code),
    CONSTRAINT ck_ic_not_self CHECK (from_area_code <> to_area_code)
);
GO

/* ---------- ref.production_type --------------------------------------
   ENTSO-E PSR (Power System Resource) codes -> business categories.
   is_renewable is a BUSINESS decision (waste? pumped storage?) — that's
   why it lives in an editable table, not in code.

   Sustainability: lifecycle_gco2e_per_kwh = lifecycle greenhouse-gas
   emissions per kWh generated, IPCC AR5 (2014) WG3 Annex III MEDIAN values.
   Used to estimate grid carbon intensity (gCO2e/kWh) per country and hour.
   - Lignite / coal-derived gas have no separate IPCC value -> proxy = coal
     (820), a documented, conservative (probably low) estimate.
   - Oil, oil shale, peat, waste, "other": no IPCC AR5 value -> NULL.
     Their share of generation is reported as "factor coverage %", so the
     gap is visible instead of silently hidden.
   - Storage (pumped hydro, batteries) -> NULL on purpose: it only shifts
     energy in time; counting it would double-count the charging source.
   Limitation: this is PRODUCTION-based intensity (imports not included).
   --------------------------------------------------------------------- */
IF OBJECT_ID(N'ref.production_type', N'U') IS NULL
CREATE TABLE ref.production_type (
    psr_type_code   char(3)      NOT NULL,          -- e.g. 'B16'
    entsoe_name     nvarchar(60) NOT NULL,
    fuel_category   varchar(20)  NOT NULL,
    is_renewable    bit          NOT NULL,
    is_low_carbon   bit          NOT NULL,          -- renewable OR nuclear
    lifecycle_gco2e_per_kwh decimal(6, 1) NULL,     -- NULL = no reliable factor / excluded
    emission_factor_source  varchar(80)   NOT NULL, -- where the number comes from
    modified_at_utc datetime2(0) NOT NULL CONSTRAINT df_pt_mod DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT pk_production_type PRIMARY KEY (psr_type_code),
    CONSTRAINT uq_production_type_name UNIQUE (entsoe_name),
    CONSTRAINT ck_pt_category CHECK (fuel_category IN ('Fossil', 'Nuclear', 'Renewable', 'Storage', 'Other'))
);
GO

/* ---------- ref.weather_location -------------------------------------
   Weather is measured at POINTS; electricity is reported per COUNTRY.
   We sample a few representative points per country and combine them
   with a weight. Equal weights for now — a documented simplification
   (population- or capacity-weighting is a later improvement).
   --------------------------------------------------------------------- */
IF OBJECT_ID(N'ref.weather_location', N'U') IS NULL
CREATE TABLE ref.weather_location (
    location_code   varchar(10)   NOT NULL,         -- e.g. 'IT-MIL'
    country_code    char(2)       NOT NULL,
    location_name   nvarchar(50)  NOT NULL,
    latitude        decimal(8, 5) NOT NULL,
    longitude       decimal(8, 5) NOT NULL,
    weight          decimal(5, 4) NOT NULL,         -- weights per country sum to 1
    modified_at_utc datetime2(0)  NOT NULL CONSTRAINT df_wl_mod DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT pk_weather_location PRIMARY KEY (location_code),
    CONSTRAINT fk_wl_country FOREIGN KEY (country_code) REFERENCES ref.country (country_code),
    CONSTRAINT ck_wl_lat CHECK (latitude  BETWEEN -90  AND 90),
    CONSTRAINT ck_wl_lon CHECK (longitude BETWEEN -180 AND 180),
    CONSTRAINT ck_wl_weight CHECK (weight > 0 AND weight <= 1)
);
GO

/* ---------- sec.user_country -----------------------------------------
   Who may see which country. Used for Row-Level Security in Gold later.
   Empty for now — filled once test users exist.
   --------------------------------------------------------------------- */
IF OBJECT_ID(N'sec.user_country', N'U') IS NULL
CREATE TABLE sec.user_country (
    user_principal_name nvarchar(256) NOT NULL,     -- Entra UPN
    country_code        char(2)       NOT NULL,
    modified_at_utc     datetime2(0)  NOT NULL CONSTRAINT df_uc_mod DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT pk_user_country PRIMARY KEY (user_principal_name, country_code),
    CONSTRAINT fk_uc_country FOREIGN KEY (country_code) REFERENCES ref.country (country_code)
);
GO

/* =====================================================================
   SEED DATA — one transaction, idempotent MERGEs.
   Only UPDATE when a value actually changed: re-runs then generate no
   change traffic (which mirroring would replicate into Fabric).
   ===================================================================== */
BEGIN TRANSACTION;

/* --- 12 countries; pilot = IT, FR, DE, NL (is_active set on INSERT only) --- */
MERGE ref.country AS tgt
USING (VALUES
    ('IT', N'Italy',          'Europe/Rome',      1),
    ('FR', N'France',         'Europe/Paris',     1),
    ('DE', N'Germany',        'Europe/Berlin',    1),
    ('ES', N'Spain',          'Europe/Madrid',    0),
    ('PT', N'Portugal',       'Europe/Lisbon',    0),
    ('CH', N'Switzerland',    'Europe/Zurich',    0),
    ('AT', N'Austria',        'Europe/Vienna',    0),
    ('BE', N'Belgium',        'Europe/Brussels',  0),
    ('NL', N'Netherlands',    'Europe/Amsterdam', 1),
    ('PL', N'Poland',         'Europe/Warsaw',    0),
    ('CZ', N'Czech Republic', 'Europe/Prague',    0),
    ('SI', N'Slovenia',       'Europe/Ljubljana', 0)
) AS src (country_code, country_name, time_zone, is_active)
ON tgt.country_code = src.country_code
WHEN MATCHED AND (tgt.country_name <> src.country_name OR tgt.time_zone <> src.time_zone) THEN
    UPDATE SET country_name = src.country_name,
               time_zone = src.time_zone,
               modified_at_utc = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (country_code, country_name, time_zone, is_active)
    VALUES (src.country_code, src.country_name, src.time_zone, src.is_active);

/* --- areas (EIC codes as mapped in the entsoe-py library) --- */
MERGE ref.area AS tgt
USING (VALUES
    -- area_code, eic_code,           area_name,                     cc,  country, bz
    ('IT',      '10YIT-GRTN-----B', N'Italy (control area)',          'IT', 1, 0),
    ('IT_NORD', '10Y1001A1001A73I', N'IT-North bidding zone',         'IT', 0, 1),
    ('IT_CNOR', '10Y1001A1001A70O', N'IT-Centre-North bidding zone',  'IT', 0, 1),
    ('IT_CSUD', '10Y1001A1001A71M', N'IT-Centre-South bidding zone',  'IT', 0, 1),
    ('IT_SUD',  '10Y1001A1001A788', N'IT-South bidding zone',         'IT', 0, 1),
    ('IT_SICI', '10Y1001A1001A75E', N'IT-Sicily bidding zone',        'IT', 0, 1),
    ('IT_SARD', '10Y1001A1001A74G', N'IT-Sardinia bidding zone',      'IT', 0, 1),
    ('IT_CALA', '10Y1001C--00096J', N'IT-Calabria bidding zone',      'IT', 0, 1),
    ('DE',      '10Y1001A1001A83F', N'Germany',                       'DE', 1, 0),
    ('DE_LU',   '10Y1001A1001A82H', N'DE-LU bidding zone',            'DE', 0, 1),
    ('FR',      '10YFR-RTE------C', N'France',                        'FR', 1, 1),
    ('ES',      '10YES-REE------0', N'Spain',                         'ES', 1, 1),
    ('PT',      '10YPT-REN------W', N'Portugal',                      'PT', 1, 1),
    ('CH',      '10YCH-SWISSGRIDZ', N'Switzerland',                   'CH', 1, 1),
    ('AT',      '10YAT-APG------L', N'Austria',                       'AT', 1, 1),
    ('BE',      '10YBE----------2', N'Belgium',                       'BE', 1, 1),
    ('NL',      '10YNL----------L', N'Netherlands',                   'NL', 1, 1),
    ('PL',      '10YPL-AREA-----S', N'Poland',                        'PL', 1, 1),
    ('CZ',      '10YCZ-CEPS-----N', N'Czech Republic',                'CZ', 1, 1),
    ('SI',      '10YSI-ELES-----O', N'Slovenia',                      'SI', 1, 1)
) AS src (area_code, eic_code, area_name, country_code, is_country_area, is_bidding_zone)
ON tgt.area_code = src.area_code
WHEN MATCHED AND (tgt.eic_code <> src.eic_code
               OR tgt.area_name <> src.area_name
               OR tgt.country_code <> src.country_code
               OR tgt.is_country_area <> src.is_country_area
               OR tgt.is_bidding_zone <> src.is_bidding_zone) THEN
    UPDATE SET eic_code = src.eic_code,
               area_name = src.area_name,
               country_code = src.country_code,
               is_country_area = src.is_country_area,
               is_bidding_zone = src.is_bidding_zone,
               modified_at_utc = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (area_code, eic_code, area_name, country_code, is_country_area, is_bidding_zone)
    VALUES (src.area_code, src.eic_code, src.area_name, src.country_code, src.is_country_area, src.is_bidding_zone);

/* --- dataset -> area mapping, derived from the area roles --- */
WITH mapping AS (
    SELECT d.dataset, a.area_code
    FROM ref.area AS a
    CROSS JOIN (VALUES ('actual_load'), ('actual_generation'), ('installed_capacity')) AS d (dataset)
    WHERE a.is_country_area = 1
    UNION ALL
    SELECT 'day_ahead_price', a.area_code
    FROM ref.area AS a
    WHERE a.is_bidding_zone = 1
)
MERGE ref.dataset_area AS tgt
USING mapping AS src
ON tgt.dataset = src.dataset AND tgt.area_code = src.area_code
WHEN NOT MATCHED BY TARGET THEN
    INSERT (dataset, area_code) VALUES (src.dataset, src.area_code);

/* --- 20 physical borders between the 12 country areas, both directions (40 rows) --- */
WITH borders (a, b) AS (
    SELECT a, b FROM (VALUES
        ('IT','FR'), ('IT','CH'), ('IT','AT'), ('IT','SI'),
        ('FR','BE'), ('FR','DE'), ('FR','ES'), ('FR','CH'),
        ('ES','PT'),
        ('DE','NL'), ('DE','BE'), ('DE','CH'), ('DE','AT'), ('DE','CZ'), ('DE','PL'),
        ('AT','CH'), ('AT','CZ'), ('AT','SI'),
        ('BE','NL'),
        ('PL','CZ')
    ) AS v (a, b)
),
directed (from_area_code, to_area_code) AS (
    SELECT a, b FROM borders
    UNION ALL
    SELECT b, a FROM borders
)
MERGE ref.interconnector AS tgt
USING directed AS src
ON tgt.from_area_code = src.from_area_code AND tgt.to_area_code = src.to_area_code
WHEN NOT MATCHED BY TARGET THEN
    INSERT (from_area_code, to_area_code) VALUES (src.from_area_code, src.to_area_code);

/* --- ENTSO-E PSR types + IPCC AR5 lifecycle emission factors (gCO2e/kWh, median) --- */
MERGE ref.production_type AS tgt
USING (VALUES
  -- code   entsoe_name                          category     renew low_c  gCO2e  source
    ('B01', N'Biomass',                          'Renewable', 1, 1, 230.0, 'IPCC AR5 median: biomass (dedicated)'),
    ('B02', N'Fossil Brown coal/Lignite',        'Fossil',    0, 0, 820.0, 'Proxy: IPCC AR5 coal (PC); lignite likely higher'),
    ('B03', N'Fossil Coal-derived gas',          'Fossil',    0, 0, 820.0, 'Proxy: IPCC AR5 coal (PC)'),
    ('B04', N'Fossil Gas',                       'Fossil',    0, 0, 490.0, 'IPCC AR5 median: gas combined cycle'),
    ('B05', N'Fossil Hard coal',                 'Fossil',    0, 0, 820.0, 'IPCC AR5 median: coal (PC)'),
    ('B06', N'Fossil Oil',                       'Fossil',    0, 0, NULL,  'No IPCC AR5 value'),
    ('B07', N'Fossil Oil shale',                 'Fossil',    0, 0, NULL,  'No IPCC AR5 value'),
    ('B08', N'Fossil Peat',                      'Fossil',    0, 0, NULL,  'No IPCC AR5 value'),
    ('B09', N'Geothermal',                       'Renewable', 1, 1, 38.0,  'IPCC AR5 median: geothermal'),
    ('B10', N'Hydro Pumped Storage',             'Storage',   0, 0, NULL,  'Excluded: storage shifts energy'),
    ('B11', N'Hydro Run-of-river and poundage',  'Renewable', 1, 1, 24.0,  'IPCC AR5 median: hydropower'),
    ('B12', N'Hydro Water Reservoir',            'Renewable', 1, 1, 24.0,  'IPCC AR5 median: hydropower'),
    ('B13', N'Marine',                           'Renewable', 1, 1, 17.0,  'IPCC AR5 median: ocean (tidal/wave)'),
    ('B14', N'Nuclear',                          'Nuclear',   0, 1, 12.0,  'IPCC AR5 median: nuclear'),
    ('B15', N'Other renewable',                  'Renewable', 1, 1, NULL,  'No single IPCC AR5 value'),
    ('B16', N'Solar',                            'Renewable', 1, 1, 48.0,  'IPCC AR5 median: solar PV (utility)'),
    ('B17', N'Waste',                            'Other',     0, 0, NULL,  'No IPCC AR5 value'),
    ('B18', N'Wind Offshore',                    'Renewable', 1, 1, 12.0,  'IPCC AR5 median: wind offshore'),
    ('B19', N'Wind Onshore',                     'Renewable', 1, 1, 11.0,  'IPCC AR5 median: wind onshore'),
    ('B20', N'Other',                            'Other',     0, 0, NULL,  'No IPCC AR5 value'),
    ('B25', N'Energy storage',                   'Storage',   0, 0, NULL,  'Excluded: storage shifts energy')
) AS src (psr_type_code, entsoe_name, fuel_category, is_renewable, is_low_carbon,
          lifecycle_gco2e_per_kwh, emission_factor_source)
ON tgt.psr_type_code = src.psr_type_code
WHEN MATCHED AND (tgt.entsoe_name   <> src.entsoe_name
               OR tgt.fuel_category <> src.fuel_category
               OR tgt.is_renewable  <> src.is_renewable
               OR tgt.is_low_carbon <> src.is_low_carbon
               OR ISNULL(tgt.lifecycle_gco2e_per_kwh, -1) <> ISNULL(src.lifecycle_gco2e_per_kwh, -1)
               OR tgt.emission_factor_source <> src.emission_factor_source) THEN
    UPDATE SET entsoe_name             = src.entsoe_name,
               fuel_category           = src.fuel_category,
               is_renewable            = src.is_renewable,
               is_low_carbon           = src.is_low_carbon,
               lifecycle_gco2e_per_kwh = src.lifecycle_gco2e_per_kwh,
               emission_factor_source  = src.emission_factor_source,
               modified_at_utc         = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (psr_type_code, entsoe_name, fuel_category, is_renewable, is_low_carbon,
            lifecycle_gco2e_per_kwh, emission_factor_source)
    VALUES (src.psr_type_code, src.entsoe_name, src.fuel_category, src.is_renewable, src.is_low_carbon,
            src.lifecycle_gco2e_per_kwh, src.emission_factor_source);

/* --- weather points: 1-3 per country, equal weight within a country --- */
MERGE ref.weather_location AS tgt
USING (VALUES
    ('IT-MIL', 'IT', N'Milan',     45.46420,   9.19000, 0.3333),
    ('IT-ROM', 'IT', N'Rome',      41.90280,  12.49640, 0.3333),
    ('IT-PAL', 'IT', N'Palermo',   38.11570,  13.36150, 0.3334),
    ('FR-PAR', 'FR', N'Paris',     48.85660,   2.35220, 0.3333),
    ('FR-LYS', 'FR', N'Lyon',      45.76400,   4.83570, 0.3333),
    ('FR-TLS', 'FR', N'Toulouse',  43.60470,   1.44420, 0.3334),
    ('DE-BER', 'DE', N'Berlin',    52.52000,  13.40500, 0.3333),
    ('DE-HAM', 'DE', N'Hamburg',   53.55110,   9.99370, 0.3333),
    ('DE-MUC', 'DE', N'Munich',    48.13510,  11.58200, 0.3334),
    ('ES-MAD', 'ES', N'Madrid',    40.41680,  -3.70380, 0.3333),
    ('ES-SVQ', 'ES', N'Seville',   37.38910,  -5.98450, 0.3333),
    ('ES-ZAZ', 'ES', N'Zaragoza',  41.64880,  -0.88910, 0.3334),
    ('PT-LIS', 'PT', N'Lisbon',    38.72230,  -9.13930, 0.5000),
    ('PT-OPO', 'PT', N'Porto',     41.15790,  -8.62910, 0.5000),
    ('CH-ZRH', 'CH', N'Zurich',    47.37690,   8.54170, 1.0000),
    ('AT-VIE', 'AT', N'Vienna',    48.20820,  16.37380, 1.0000),
    ('BE-BRU', 'BE', N'Brussels',  50.85030,   4.35170, 1.0000),
    ('NL-AMS', 'NL', N'Amsterdam', 52.36760,   4.90410, 0.5000),
    ('NL-GRQ', 'NL', N'Groningen', 53.21940,   6.56650, 0.5000),
    ('PL-WAW', 'PL', N'Warsaw',    52.22970,  21.01220, 0.5000),
    ('PL-GDN', 'PL', N'Gdansk',    54.35200,  18.64660, 0.5000),
    ('CZ-PRG', 'CZ', N'Prague',    50.07550,  14.43780, 1.0000),
    ('SI-LJU', 'SI', N'Ljubljana', 46.05690,  14.50580, 1.0000)
) AS src (location_code, country_code, location_name, latitude, longitude, weight)
ON tgt.location_code = src.location_code
WHEN MATCHED AND (tgt.location_name <> src.location_name
               OR tgt.latitude  <> src.latitude
               OR tgt.longitude <> src.longitude
               OR tgt.weight    <> src.weight) THEN
    UPDATE SET location_name = src.location_name,
               latitude = src.latitude,
               longitude = src.longitude,
               weight = src.weight,
               modified_at_utc = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (location_code, country_code, location_name, latitude, longitude, weight)
    VALUES (src.location_code, src.country_code, src.location_name, src.latitude, src.longitude, src.weight);

COMMIT TRANSACTION;
GO

/* =====================================================================
   Grant the Fabric mirroring login least-privilege access to THIS DB.
   (Login created in master by 01_master_fabric_login.sql.)
   ===================================================================== */
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'fabric_mirror_user')
    CREATE USER [fabric_mirror_user] FOR LOGIN [fabric_mirror_login];
GO
GRANT SELECT,
      ALTER ANY EXTERNAL MIRROR,
      VIEW DATABASE PERFORMANCE STATE,
      VIEW DATABASE SECURITY STATE
TO [fabric_mirror_user];
GO

/* =====================================================================
   Verify. Expected row counts:
     ref.country 12 (4 active) | ref.area 20 | ref.dataset_area 55
     ref.interconnector 40 | ref.production_type 21
     ref.weather_location 23 | sec.user_country 0
   (dataset_area = 12 country areas x 3 datasets + 19 bidding zones)
   Weights check: every country's weights should sum to 1.0000.
   ===================================================================== */
SELECT 'ref.country' AS table_name, COUNT(*) AS row_count,
       SUM(CAST(is_active AS int)) AS active_rows FROM ref.country
UNION ALL SELECT 'ref.area',             COUNT(*), NULL FROM ref.area
UNION ALL SELECT 'ref.dataset_area',     COUNT(*), NULL FROM ref.dataset_area
UNION ALL SELECT 'ref.interconnector',   COUNT(*), NULL FROM ref.interconnector
UNION ALL SELECT 'ref.production_type',  COUNT(*), NULL FROM ref.production_type
UNION ALL SELECT 'ref.weather_location', COUNT(*), NULL FROM ref.weather_location
UNION ALL SELECT 'sec.user_country',     COUNT(*), NULL FROM sec.user_country;

SELECT country_code, SUM(weight) AS total_weight
FROM ref.weather_location
GROUP BY country_code
HAVING SUM(weight) <> 1.0000;   -- expect zero rows

-- What the pipeline will actually ingest right now (pilot scope):
SELECT da.dataset, a.area_code, a.eic_code
FROM ref.dataset_area AS da
JOIN ref.area    AS a ON a.area_code = da.area_code
JOIN ref.country AS c ON c.country_code = a.country_code
WHERE da.is_enabled = 1 AND c.is_active = 1
ORDER BY da.dataset, a.area_code;
GO
