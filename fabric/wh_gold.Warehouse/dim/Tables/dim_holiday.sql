CREATE TABLE [dim].[dim_holiday] (
    [country_code]  CHAR (2)      NULL,
    [date_key]      INT           NULL,
    [holiday_date]  DATE          NOT NULL,
    [holiday_name]  VARCHAR (100) NULL,
    [local_name]    VARCHAR (100) NULL,
    [is_nationwide] BIT           NULL
);


GO