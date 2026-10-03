/* ---------- ONE entry point for the pipeline: dims -> facts -> reconciliation ---------- */
CREATE     PROCEDURE ops.usp_load_gold
    @from_date date = NULL          -- NULL = each fact uses its own recent window
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @started    datetime2(0) = CAST(SYSUTCDATETIME() AS datetime2(0));
    DECLARE @check_from date = COALESCE(@from_date, DATEADD(day, -7, CAST(SYSUTCDATETIME() AS date)));

    EXEC dim.usp_refresh_dims;
    EXEC dim.usp_scd2_emission_factor;
    EXEC fact.usp_load_fact_load_hourly       @from_date;
    EXEC fact.usp_load_fact_generation_hourly @from_date;
    EXEC fact.usp_load_fact_carbon_hourly     @from_date;   -- must run after generation
    EXEC fact.usp_load_fact_price_hourly      @from_date;
    EXEC fact.usp_load_fact_flow_hourly       @from_date;
    EXEC fact.usp_load_fact_weather_hourly    @from_date;
    

    -- reconciliation: Gold load MWh must equal Silver (tolerance = decimal rounding)
    DECLARE @gold   decimal(18,3) = (SELECT SUM(load_mwh)   FROM fact.fact_load_hourly    WHERE ts_hour_utc >= @check_from);
    DECLARE @silver decimal(18,3) = (SELECT CAST(SUM(energy_mwh) AS decimal(18,3)) FROM lh_silver.dbo.silver_load WHERE ts_utc >= @check_from);
    DECLARE @diff   decimal(18,3) = ABS(ISNULL(@gold, 0) - ISNULL(@silver, 0));

    INSERT INTO ops.gold_run_log
    VALUES (@started, CAST(SYSUTCDATETIME() AS datetime2(0)), @check_from,
            CASE WHEN @diff <= 1 THEN 'OK' ELSE 'FAILED' END, @diff,
            CASE WHEN @diff <= 1 THEN 'Gold reconciled with Silver' ELSE 'Load MWh differs between Gold and Silver' END);

    IF @diff > 1
        THROW 50001, 'Gold reconciliation failed: load MWh differs from Silver', 1;   -- fails the pipeline -> alert
END;

GO