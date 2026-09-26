CREATE TABLE [dim].[dim_date] (
    [date_key]     INT          NULL,
    [date]         DATE         NULL,
    [year]         INT          NULL,
    [quarter]      INT          NULL,
    [month]        INT          NULL,
    [month_name]   VARCHAR (10) NULL,
    [day_of_month] INT          NULL,
    [day_of_week]  INT          NULL,
    [day_name]     VARCHAR (10) NULL,
    [is_weekend]   BIT          NULL,
    [season]       VARCHAR (6)  NULL
);


GO