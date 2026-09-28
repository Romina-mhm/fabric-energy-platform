CREATE FUNCTION sec.fn_country_filter (@country_code char(2))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
    SELECT 1 AS allowed
    FROM sec.user_country uc
    WHERE uc.user_name = USER_NAME()
      AND (uc.country_code = @country_code OR uc.country_code = '*');

GO