-- 2) Load procedure: rebuild everything from @from_date (default = last 7 days)
CREATE   PROCEDURE fact.usp_load_fact_load_hourly
    @from_date date = NULL
AS
BEGIN
    SET NOCOUNT ON;
    IF @from_date IS NULL
        SET @from_date = DATEADD(day, -7, CAST(SYSUTCDATETIME() AS date));

    BEGIN TRANSACTION;

    DELETE FROM fact.fact_load_hourly
    WHERE ts_hour_utc >= @from_date;

    INSERT INTO fact.fact_load_hourly
        (date_key, hour_key, ts_hour_utc, area_code, country_code,
         load_mwh, avg_load_mw, coverage_minutes, loaded_at_utc)
    SELECT
        CAST(CONVERT(varchar(8), h.ts_hour, 112) AS int),
        CAST(DATEPART(hour, h.ts_hour) AS smallint),
        h.ts_hour,
        h.area_code,
        a.country_code,
        CAST(h.load_mwh AS decimal(14,3)),
        CAST(h.load_mwh * 60.0 / h.coverage_minutes AS decimal(14,3)),   -- average MW over covered minutes
        h.coverage_minutes,
        CAST(SYSUTCDATETIME() AS datetime2(0))
    FROM (
        SELECT CAST(DATETRUNC(hour, s.ts_utc) AS datetime2(0)) AS ts_hour,
               s.area_code,
               SUM(s.energy_mwh)         AS load_mwh,
               SUM(s.resolution_minutes) AS coverage_minutes
        FROM lh_silver.dbo.silver_load s
        WHERE s.ts_utc >= @from_date
        GROUP BY CAST(DATETRUNC(hour, s.ts_utc) AS datetime2(0)), s.area_code
    ) h
    JOIN dim.dim_area a ON a.area_code = h.area_code;

    COMMIT TRANSACTION;
END;

GO