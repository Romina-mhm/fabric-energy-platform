/* ---------- refresh reference dimensions (they come from the mirrored Azure SQL DB) ---------- */
CREATE   PROCEDURE dim.usp_refresh_dims
AS
BEGIN
    SET NOCOUNT ON;

    DROP TABLE IF EXISTS dim.dim_country;
    CREATE TABLE dim.dim_country AS
    SELECT CAST(country_code AS char(2))     AS country_code,
           CAST(country_name AS varchar(50)) AS country_name,
           CAST(time_zone    AS varchar(40)) AS time_zone,
           CAST(is_active    AS bit)         AS is_active
    FROM lh_bronze.dbo.country;

    DROP TABLE IF EXISTS dim.dim_area;
    CREATE TABLE dim.dim_area AS
    SELECT CAST(a.area_code AS varchar(10))    AS area_code,
           CAST(a.eic_code  AS varchar(16))    AS eic_code,
           CAST(a.area_name AS varchar(60))    AS area_name,
           CAST(a.country_code AS char(2))     AS country_code,
           CAST(c.country_name AS varchar(50)) AS country_name,
           CAST(a.is_country_area AS bit)      AS is_country_area,
           CAST(a.is_bidding_zone AS bit)      AS is_bidding_zone
    FROM lh_bronze.dbo.area a
    JOIN lh_bronze.dbo.country c ON c.country_code = a.country_code;

    DROP TABLE IF EXISTS dim.dim_production_type;
    CREATE TABLE dim.dim_production_type AS
    SELECT CAST(psr_type_code AS char(3))           AS psr_type_code,
           CAST(entsoe_name   AS varchar(60))       AS production_type_name,
           CAST(fuel_category AS varchar(20))       AS fuel_category,
           CAST(is_renewable  AS bit)               AS is_renewable,
           CAST(is_low_carbon AS bit)               AS is_low_carbon,
           CAST(lifecycle_gco2e_per_kwh AS decimal(6,1)) AS lifecycle_gco2e_per_kwh,
           CAST(CASE WHEN lifecycle_gco2e_per_kwh IS NULL THEN 0 ELSE 1 END AS bit) AS has_emission_factor,
           CAST(emission_factor_source AS varchar(80)) AS emission_factor_source
    FROM lh_bronze.dbo.production_type;

    DROP TABLE IF EXISTS dim.dim_holiday;
    CREATE TABLE dim.dim_holiday AS
    SELECT CAST(country_code AS char(2))                          AS country_code,
           CAST(CONVERT(varchar(8), holiday_date, 112) AS int)    AS date_key,
           holiday_date,
           CAST(name       AS varchar(100))                       AS holiday_name,
           CAST(local_name AS varchar(100))                       AS local_name,
           CAST(is_nationwide AS bit)                             AS is_nationwide
    FROM lh_silver.dbo.silver_holiday;
END;

GO