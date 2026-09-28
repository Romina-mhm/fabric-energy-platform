CREATE SECURITY POLICY [sec].[country_policy]
    ADD FILTER PREDICATE [sec].[fn_country_filter]([country_code]) ON [fact].[fact_load_hourly],
    ADD FILTER PREDICATE [sec].[fn_country_filter]([country_code]) ON [fact].[fact_carbon_hourly]
    WITH (STATE = OFF);


GO