CREATE TABLE [fact].[fact_load_hourly] (
    [date_key]         INT             NOT NULL,
    [hour_key]         SMALLINT        NOT NULL,
    [ts_hour_utc]      DATETIME2 (0)   NOT NULL,
    [area_code]        VARCHAR (10)    NOT NULL,
    [country_code]     CHAR (2)        NOT NULL,
    [load_mwh]         DECIMAL (14, 3) NOT NULL,
    [avg_load_mw]      DECIMAL (14, 3) NOT NULL,
    [coverage_minutes] INT             NOT NULL,
    [loaded_at_utc]    DATETIME2 (0)   NOT NULL
);


GO