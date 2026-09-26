CREATE   PROCEDURE fact.usp_load_fact_price_hourly
    @from_date date = NULL
AS
BEGIN
    SET NOCOUNT ON;
    IF @from_date IS NULL SET @from_date = DATEADD(day, -7, CAST(SYSUTCDATETIME() AS date));

    BEGIN TRANSACTION;
    DELETE FROM fact.fact_price_hourly WHERE ts_hour_utc >= @from_date;

    INSERT INTO fact.fact_price_hourly
        (date_key, hour_key, ts_hour_utc, area_code, country_code, price_eur_mwh,
         min_price_eur_mwh, max_price_eur_mwh, source_resolution_minutes, source_slots, loaded_at_utc)
    SELECT
        CAST(CONVERT(varchar(8), r.ts_hour, 112) AS int),
        CAST(DATEPART(hour, r.ts_hour) AS smallint),
        r.ts_hour, r.area_code, a.country_code,
        CAST(r.avg_price AS decimal(10,2)),
        CAST(r.min_price AS decimal(10,2)),
        CAST(r.max_price AS decimal(10,2)),
        CAST(r.resolution_minutes AS smallint),
        CAST(r.slots AS smallint),
        CAST(SYSUTCDATETIME() AS datetime2(0))
    FROM (
        SELECT b.*,
               ROW_NUMBER() OVER (PARTITION BY b.area_code, b.ts_hour
                                  ORDER BY CASE WHEN b.resolution_minutes = 60 THEN 0 ELSE 1 END) AS rn
        FROM (
            SELECT area_code,
                   CAST(DATETRUNC(hour, ts_utc) AS datetime2(0)) AS ts_hour,
                   resolution_minutes,
                   AVG(price_eur_mwh) AS avg_price,
                   MIN(price_eur_mwh) AS min_price,
                   MAX(price_eur_mwh) AS max_price,
                   COUNT(*)           AS slots
            FROM lh_silver.dbo.silver_price
            WHERE ts_utc >= @from_date
            GROUP BY area_code, CAST(DATETRUNC(hour, ts_utc) AS datetime2(0)), resolution_minutes
        ) b
    ) r
    JOIN dim.dim_area a ON a.area_code = r.area_code
    WHERE r.rn = 1;
    COMMIT TRANSACTION;
END;

GO