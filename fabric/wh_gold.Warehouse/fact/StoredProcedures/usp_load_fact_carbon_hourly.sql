CREATE   PROCEDURE fact.usp_load_fact_carbon_hourly
    @from_date date = NULL
AS
BEGIN
    SET NOCOUNT ON;
    IF @from_date IS NULL SET @from_date = DATEADD(day, -7, CAST(SYSUTCDATETIME() AS date));

    BEGIN TRANSACTION;
    DELETE FROM fact.fact_carbon_hourly WHERE ts_hour_utc >= @from_date;

    INSERT INTO fact.fact_carbon_hourly
        (date_key, hour_key, ts_hour_utc, country_code, total_generation_mwh, renewable_mwh,
         low_carbon_mwh, factor_covered_mwh, co2e_kg, renewable_share_pct, low_carbon_share_pct,
         carbon_intensity_g_per_kwh, factor_coverage_pct, loaded_at_utc)
    SELECT
        x.date_key, x.hour_key, x.ts_hour_utc, x.country_code,
        x.total_mwh, x.renewable_mwh, x.low_carbon_mwh, x.covered_mwh, x.co2e_kg,
        CAST(100.0 * x.renewable_mwh  / NULLIF(x.total_mwh, 0)   AS decimal(6,2)),
        CAST(100.0 * x.low_carbon_mwh / NULLIF(x.total_mwh, 0)   AS decimal(6,2)),
        CAST(x.co2e_kg                / NULLIF(x.covered_mwh, 0) AS decimal(8,2)),
        CAST(100.0 * x.covered_mwh    / NULLIF(x.total_mwh, 0)   AS decimal(6,2)),
        CAST(SYSUTCDATETIME() AS datetime2(0))
    FROM (
        SELECT f.date_key, f.hour_key, f.ts_hour_utc, f.country_code,
               SUM(f.generation_mwh)                                                AS total_mwh,
               SUM(CASE WHEN p.is_renewable = 1        THEN f.generation_mwh ELSE 0 END) AS renewable_mwh,
               SUM(CASE WHEN p.is_low_carbon = 1       THEN f.generation_mwh ELSE 0 END) AS low_carbon_mwh,
               SUM(CASE WHEN p.has_emission_factor = 1 THEN f.generation_mwh ELSE 0 END) AS covered_mwh,
               SUM(f.co2e_kg)                                                       AS co2e_kg
        FROM fact.fact_generation_hourly f
        JOIN dim.dim_production_type p ON p.psr_type_code = f.psr_type_code
        WHERE f.ts_hour_utc >= @from_date
        GROUP BY f.date_key, f.hour_key, f.ts_hour_utc, f.country_code
    ) x;
    COMMIT TRANSACTION;
END;

GO