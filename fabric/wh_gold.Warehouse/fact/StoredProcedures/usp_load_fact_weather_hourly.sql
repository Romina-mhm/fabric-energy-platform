CREATE   PROCEDURE fact.usp_load_fact_weather_hourly
    @from_date date = NULL
AS
BEGIN
    SET NOCOUNT ON;
    IF @from_date IS NULL SET @from_date = DATEADD(day, -10, CAST(SYSUTCDATETIME() AS date));  -- archive lags ~6 days

    BEGIN TRANSACTION;
    DELETE FROM fact.fact_weather_hourly WHERE ts_hour_utc >= @from_date;

    INSERT INTO fact.fact_weather_hourly
        (date_key, hour_key, ts_hour_utc, country_code, temperature_c, wind_speed_100m_kmh,
         solar_radiation_wm2, cloud_cover_pct, precipitation_mm, archive_share, is_observed, loaded_at_utc)
    SELECT
        CAST(CONVERT(varchar(8), w.ts_utc, 112) AS int),
        CAST(DATEPART(hour, w.ts_utc) AS smallint),
        CAST(w.ts_utc AS datetime2(0)),
        CAST(w.country_code AS char(2)),
        CAST(w.temperature_2m      AS decimal(6,2)),
        CAST(w.wind_speed_100m     AS decimal(6,2)),
        CAST(w.shortwave_radiation AS decimal(8,2)),
        CAST(w.cloud_cover         AS decimal(6,2)),
        CAST(w.precipitation       AS decimal(6,2)),
        CAST(w.archive_share       AS decimal(4,3)),
        CAST(CASE WHEN w.archive_share >= 0.999 THEN 1 ELSE 0 END AS bit),
        CAST(SYSUTCDATETIME() AS datetime2(0))
    FROM lh_silver.dbo.silver_weather_country_hourly w
    WHERE w.ts_utc >= @from_date;
    COMMIT TRANSACTION;
END;

GO