/*
-- @title: ENS - Price
-- @description: Provides the last recorded ENS price in USD for each day over the last year.
-- @author: Steakhouse Financial
-- @notes: N/A
-- @version:
    - 2.0 - 2024-02-19 - Added comment header, updated query formatting
    - 1.0 - 2023-06-16 - Initial version
*/

WITH
    daily_prices AS (
        SELECT
            minute AS period,
            price,
            ROW_NUMBER() OVER(PARTITION BY DATE(minute) ORDER BY minute DESC) as rn
        FROM prices.usd
        WHERE contract_address = 0xc18360217d8f7ab5e7c516566761ea12ce7f9d72 -- ENS Token
          AND minute > CURRENT_DATE - interval '365' day
)

SELECT
    period,
    price AS "ENS Price"
FROM daily_prices
WHERE rn = 1
ORDER BY period DESC;