CREATE     PROCEDURE dim.usp_refresh_dims
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRANSACTION;

    TRUNCATE TABLE dim.dim_country;
    INSERT INTO dim.dim_country (country_code, country_name, time_zone, is_active)
    SELECT CAST(country_code AS char(2)), CAST(country_name AS varchar(50)),
           CAST(time_zone AS varchar(40)), CAST(is_active AS bit)
    FROM lh_bronze.dbo.country;

    TRUNCATE TABLE dim.dim_area;
    INSERT INTO dim.dim_area (area_code, eic_code, area_name, country_code, country_name, is_country_area, is_bidding_zone)
    SELECT CAST(a.area_code AS varchar(10)), CAST(a.eic_code AS varchar(16)), CAST(a.area_name AS varchar(60)),
           CAST(a.country_code AS char(2)), CAST(c.country_name AS varchar(50)),
           CAST(a.is_country_area AS bit), CAST(a.is_bidding_zone AS bit)
    FROM lh_bronze.dbo.area a JOIN lh_bronze.dbo.country c ON c.country_code = a.country_code;

    TRUNCATE TABLE dim.dim_production_type;
    INSERT INTO dim.dim_production_type (psr_type_code, production_type_name, fuel_category, is_renewable,
                                         is_low_carbon, lifecycle_gco2e_per_kwh, has_emission_factor, emission_factor_source)
    SELECT CAST(psr_type_code AS char(3)), CAST(entsoe_name AS varchar(60)), CAST(fuel_category AS varchar(20)),
           CAST(is_renewable AS bit), CAST(is_low_carbon AS bit), CAST(lifecycle_gco2e_per_kwh AS decimal(6,1)),
           CAST(CASE WHEN lifecycle_gco2e_per_kwh IS NULL THEN 0 ELSE 1 END AS bit),
           CAST(emission_factor_source AS varchar(80))
    FROM lh_bronze.dbo.production_type;

    TRUNCATE TABLE dim.dim_holiday;
    INSERT INTO dim.dim_holiday (country_code, date_key, holiday_date, holiday_name, local_name, is_nationwide)
    SELECT CAST(country_code AS char(2)), CAST(CONVERT(varchar(8), holiday_date, 112) AS int), holiday_date,
           CAST(name AS varchar(100)), CAST(local_name AS varchar(100)), CAST(is_nationwide AS bit)
    FROM lh_silver.dbo.silver_holiday;

        TRUNCATE TABLE dim.dim_kpi_threshold;
    INSERT INTO dim.dim_kpi_threshold
        (country_code, carbon_intensity_alert_gco2e_per_kwh, price_spike_alert_eur_per_mwh,
         renewable_share_target_pct, valid_from, owner, note)
    SELECT CAST(country_code AS char(2)),
           CAST(carbon_intensity_alert_gco2e_per_kwh AS decimal(8,2)),
           CAST(price_spike_alert_eur_per_mwh AS decimal(10,2)),
           CAST(renewable_share_target_pct AS decimal(6,2)),
           CAST(valid_from AS date),
           CAST(owner AS varchar(100)),
           CAST(note AS varchar(400))
    FROM lh_silver.dbo.silver_kpi_threshold;

    COMMIT TRANSACTION;
END;

GO