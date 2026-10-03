CREATE TABLE [dim].[dim_emission_factor] (
    [psr_type_code]           CHAR (3)       NOT NULL,
    [lifecycle_gco2e_per_kwh] DECIMAL (6, 1) NULL,
    [emission_factor_source]  VARCHAR (80)   NULL,
    [valid_from]              DATETIME2 (0)  NOT NULL,
    [valid_to]                DATETIME2 (0)  NULL,
    [is_current]              BIT            NOT NULL
);


GO