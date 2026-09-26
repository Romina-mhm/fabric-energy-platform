CREATE   PROCEDURE fact.usp_load_fact_flow_hourly
    @from_date date = NULL
AS
BEGIN
    SET NOCOUNT ON;
    IF @from_date IS NULL SET @from_date = DATEADD(day, -7, CAST(SYSUTCDATETIME() AS date));

    BEGIN TRANSACTION;
    DELETE FROM fact.fact_flow_hourly WHERE ts_hour_utc >= @from_date;

    INSERT INTO fact.fact_flow_hourly
        (date_key, hour_key, ts_hour_utc, from_area_code, to_area_code, from_country_code,
         to_country_code, flow_mwh, source_resolution_minutes, coverage_minutes, loaded_at_utc)
    SELECT
        CAST(CONVERT(varchar(8), r.ts_hour, 112) AS int),
        CAST(DATEPART(hour, r.ts_hour) AS smallint),
        r.ts_hour, r.from_area_code, r.to_area_code, af.country_code, at.country_code,
        CAST(r.flow_mwh AS decimal(14,3)),
        CAST(r.resolution_minutes AS smallint),
        r.coverage_minutes,
        CAST(SYSUTCDATETIME() AS datetime2(0))
    FROM (
        SELECT b.*,
               ROW_NUMBER() OVER (PARTITION BY b.from_area_code, b.to_area_code, b.ts_hour
                                  ORDER BY b.coverage_minutes DESC, b.resolution_minutes ASC) AS rn
        FROM (
            SELECT from_area_code, to_area_code,
                   CAST(DATETRUNC(hour, ts_utc) AS datetime2(0)) AS ts_hour,
                   resolution_minutes,
                   SUM(energy_mwh)         AS flow_mwh,
                   SUM(resolution_minutes) AS coverage_minutes
            FROM lh_silver.dbo.silver_flow
            WHERE ts_utc >= @from_date
            GROUP BY from_area_code, to_area_code, CAST(DATETRUNC(hour, ts_utc) AS datetime2(0)), resolution_minutes
        ) b
    ) r
    JOIN dim.dim_area af ON af.area_code = r.from_area_code
    JOIN dim.dim_area at ON at.area_code = r.to_area_code
    WHERE r.rn = 1;
    COMMIT TRANSACTION;
END;

GO