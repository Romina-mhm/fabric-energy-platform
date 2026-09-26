CREATE TABLE [dim].[dim_area] (
    [area_code]       VARCHAR (10) NULL,
    [eic_code]        VARCHAR (16) NULL,
    [area_name]       VARCHAR (60) NULL,
    [country_code]    CHAR (2)     NULL,
    [country_name]    VARCHAR (50) NULL,
    [is_country_area] BIT          NULL,
    [is_bidding_zone] BIT          NULL
);


GO