CREATE   PROCEDURE dim.usp_scd2_emission_factor
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @now datetime2(0) = CAST(SYSUTCDATETIME() AS datetime2(0));
    DECLARE @first_load bit = CASE WHEN EXISTS (SELECT 1 FROM dim.dim_emission_factor) THEN 0 ELSE 1 END;

    BEGIN TRANSACTION;

    -- 1) close the current version of every factor whose value or source changed
    UPDATE dim.dim_emission_factor
    SET valid_to = @now, is_current = 0
    WHERE is_current = 1
      AND EXISTS (
          SELECT 1 FROM lh_bronze.dbo.production_type s
          WHERE s.psr_type_code = dim.dim_emission_factor.psr_type_code
            AND (   ISNULL(CAST(s.lifecycle_gco2e_per_kwh AS decimal(6,1)), -1)
                      <> ISNULL(dim.dim_emission_factor.lifecycle_gco2e_per_kwh, -1)
                 OR ISNULL(CAST(s.emission_factor_source AS varchar(80)), '')
                      <> ISNULL(dim.dim_emission_factor.emission_factor_source, '')));

    -- 2) insert a new current version for every new or changed code
    INSERT INTO dim.dim_emission_factor
        (psr_type_code, lifecycle_gco2e_per_kwh, emission_factor_source, valid_from, valid_to, is_current)
    SELECT CAST(s.psr_type_code AS char(3)),
           CAST(s.lifecycle_gco2e_per_kwh AS decimal(6,1)),
           CAST(s.emission_factor_source AS varchar(80)),
           CASE WHEN @first_load = 1 THEN CAST('2000-01-01' AS datetime2(0)) ELSE @now END,  -- first load covers all history
           NULL, 1
    FROM lh_bronze.dbo.production_type s
    WHERE NOT EXISTS (SELECT 1 FROM dim.dim_emission_factor d
                      WHERE d.psr_type_code = s.psr_type_code AND d.is_current = 1);

    COMMIT TRANSACTION;
END;

GO