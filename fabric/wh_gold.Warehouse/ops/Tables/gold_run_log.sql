CREATE TABLE [ops].[gold_run_log] (
    [run_started_utc]   DATETIME2 (0)   NOT NULL,
    [run_finished_utc]  DATETIME2 (0)   NOT NULL,
    [checked_from_date] DATE            NOT NULL,
    [status]            VARCHAR (10)    NOT NULL,
    [load_diff_mwh]     DECIMAL (18, 3) NULL,
    [message]           VARCHAR (400)   NULL
);


GO