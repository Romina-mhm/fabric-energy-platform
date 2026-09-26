CREATE TABLE [fact].[fact_carbon_hourly] (
    [date_key]                   INT             NOT NULL,
    [hour_key]                   SMALLINT        NOT NULL,
    [ts_hour_utc]                DATETIME2 (0)   NOT NULL,
    [country_code]               CHAR (2)        NOT NULL,
    [total_generation_mwh]       DECIMAL (14, 3) NOT NULL,
    [renewable_mwh]              DECIMAL (14, 3) NOT NULL,
    [low_carbon_mwh]             DECIMAL (14, 3) NOT NULL,
    [factor_covered_mwh]         DECIMAL (14, 3) NOT NULL,
    [co2e_kg]                    DECIMAL (18, 3) NULL,
    [renewable_share_pct]        DECIMAL (6, 2)  NULL,
    [low_carbon_share_pct]       DECIMAL (6, 2)  NULL,
    [carbon_intensity_g_per_kwh] DECIMAL (8, 2)  NULL,
    [factor_coverage_pct]        DECIMAL (6, 2)  NULL,
    [loaded_at_utc]              DATETIME2 (0)   NOT NULL
);


GO