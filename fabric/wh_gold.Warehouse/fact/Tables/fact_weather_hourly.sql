CREATE TABLE [fact].[fact_weather_hourly] (
    [date_key]            INT            NOT NULL,
    [hour_key]            SMALLINT       NOT NULL,
    [ts_hour_utc]         DATETIME2 (0)  NOT NULL,
    [country_code]        CHAR (2)       NOT NULL,
    [temperature_c]       DECIMAL (6, 2) NULL,
    [wind_speed_100m_kmh] DECIMAL (6, 2) NULL,
    [solar_radiation_wm2] DECIMAL (8, 2) NULL,
    [cloud_cover_pct]     DECIMAL (6, 2) NULL,
    [precipitation_mm]    DECIMAL (6, 2) NULL,
    [archive_share]       DECIMAL (4, 3) NULL,
    [is_observed]         BIT            NOT NULL,
    [loaded_at_utc]       DATETIME2 (0)  NOT NULL
);


GO