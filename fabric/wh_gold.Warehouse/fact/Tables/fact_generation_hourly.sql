CREATE TABLE [fact].[fact_generation_hourly] (
    [date_key]         INT             NOT NULL,
    [hour_key]         SMALLINT        NOT NULL,
    [ts_hour_utc]      DATETIME2 (0)   NOT NULL,
    [area_code]        VARCHAR (10)    NOT NULL,
    [country_code]     CHAR (2)        NOT NULL,
    [psr_type_code]    CHAR (3)        NOT NULL,
    [generation_mwh]   DECIMAL (14, 3) NOT NULL,
    [co2e_kg]          DECIMAL (18, 3) NULL,
    [coverage_minutes] INT             NOT NULL,
    [loaded_at_utc]    DATETIME2 (0)   NOT NULL
);


GO