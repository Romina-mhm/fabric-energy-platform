CREATE TABLE [fact].[fact_flow_hourly] (
    [date_key]                  INT             NOT NULL,
    [hour_key]                  SMALLINT        NOT NULL,
    [ts_hour_utc]               DATETIME2 (0)   NOT NULL,
    [from_area_code]            VARCHAR (10)    NOT NULL,
    [to_area_code]              VARCHAR (10)    NOT NULL,
    [from_country_code]         CHAR (2)        NOT NULL,
    [to_country_code]           CHAR (2)        NOT NULL,
    [flow_mwh]                  DECIMAL (14, 3) NOT NULL,
    [source_resolution_minutes] SMALLINT        NOT NULL,
    [coverage_minutes]          INT             NOT NULL,
    [loaded_at_utc]             DATETIME2 (0)   NOT NULL
);


GO