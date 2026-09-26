CREATE   PROCEDURE fact.usp_load_fact_generation_hourly
    @from_date date = NULL
AS
BEGIN
    SET NOCOUNT ON;
    IF @from_date IS NULL SET @from_date = DATEADD(day, -7, CAST(SYSUTCDATETIME() AS date));

    BEGIN TRANSACTION;
    DELETE FROM fact.fact_generation_hourly WHERE ts_hour_utc >= @from_date;

    INSERT INTO fact.fact_generation_hourly
        (date_key, hour_key, ts_hour_utc, area_code, country_code, psr_type_code,
         generation_mwh, co2e_kg, coverage_minutes, loaded_at_utc)
    SELECT
        CAST(CONVERT(varchar(8), g.ts_hour, 112) AS int),
        CAST(DATEPART(hour, g.ts_hour) AS smallint),
        g.ts_hour, g.area_code, a.country_code, g.psr_type_code,
        CAST(g.gen_mwh AS decimal(14,3)),
        CAST(g.co2e_kg AS decimal(18,3)),
        g.coverage_minutes,
        CAST(SYSUTCDATETIME() AS datetime2(0))
    FROM (
        SELECT CAST(DATETRUNC(hour, s.ts_utc) AS datetime2(0)) AS ts_hour,
               s.area_code,
               CAST(s.psr_type_code AS char(3)) AS psr_type_code,
               SUM(s.energy_mwh)         AS gen_mwh,
               SUM(s.co2e_kg)            AS co2e_kg,
               SUM(s.resolution_minutes) AS coverage_minutes
        FROM lh_silver.dbo.silver_generation s
        WHERE s.direction = 'generation'            -- NL/FR self-consumption rows excluded
          AND s.ts_utc >= @from_date
        GROUP BY CAST(DATETRUNC(hour, s.ts_utc) AS datetime2(0)), s.area_code, s.psr_type_code
    ) g
    JOIN dim.dim_area a ON a.area_code = g.area_code;
    COMMIT TRANSACTION;
END;

GO