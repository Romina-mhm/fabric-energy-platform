CREATE TABLE [dim].[dim_production_type] (
    [psr_type_code]           CHAR (3)       NULL,
    [production_type_name]    VARCHAR (60)   NULL,
    [fuel_category]           VARCHAR (20)   NULL,
    [is_renewable]            BIT            NULL,
    [is_low_carbon]           BIT            NULL,
    [lifecycle_gco2e_per_kwh] DECIMAL (6, 1) NULL,
    [has_emission_factor]     BIT            NULL,
    [emission_factor_source]  VARCHAR (80)   NULL
);


GO