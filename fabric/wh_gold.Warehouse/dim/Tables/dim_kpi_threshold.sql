CREATE TABLE [dim].[dim_kpi_threshold] (
    [country_code]                         CHAR (2)        NOT NULL,
    [carbon_intensity_alert_gco2e_per_kwh] DECIMAL (8, 2)  NULL,
    [price_spike_alert_eur_per_mwh]        DECIMAL (10, 2) NULL,
    [renewable_share_target_pct]           DECIMAL (6, 2)  NULL,
    [valid_from]                           DATE            NOT NULL,
    [owner]                                VARCHAR (100)   NULL,
    [note]                                 VARCHAR (400)   NULL
);


GO