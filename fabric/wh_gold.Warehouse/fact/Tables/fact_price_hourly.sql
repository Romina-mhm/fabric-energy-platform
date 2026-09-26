CREATE TABLE [fact].[fact_price_hourly] (
    [date_key]                  INT             NOT NULL,
    [hour_key]                  SMALLINT        NOT NULL,
    [ts_hour_utc]               DATETIME2 (0)   NOT NULL,
    [area_code]                 VARCHAR (10)    NOT NULL,
    [country_code]              CHAR (2)        NOT NULL,
    [price_eur_mwh]             DECIMAL (10, 2) NOT NULL,
    [min_price_eur_mwh]         DECIMAL (10, 2) NOT NULL,
    [max_price_eur_mwh]         DECIMAL (10, 2) NOT NULL,
    [source_resolution_minutes] SMALLINT        NOT NULL,
    [source_slots]              SMALLINT        NOT NULL,
    [loaded_at_utc]             DATETIME2 (0)   NOT NULL
);


GO