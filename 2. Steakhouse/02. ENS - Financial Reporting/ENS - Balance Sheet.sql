/*
-- @title: ENS - Balance Sheet
-- @description: Calculates a simplified balance sheet on a daily basis, grouping by assets (in USD and ETH),
                 liabilities, and capital, over the last three years.
-- @author: Steakhouse Financial
-- @notes: N/A
-- @version:
    - 2.0 - 2024-02-19 - Added comment header
    - 1.0 - ????-??-?? - Initial version
*/

WITH entries AS (
  SELECT CAST(account AS VARCHAR) AS account, amount, ts
  FROM query_2244104 -- result_ens_accounting_main
),
items AS (
  SELECT '11' AS rk, 'Assets - USD' AS item, DATE(ts) AS period, SUM(CASE WHEN account LIKE '11%' THEN amount END) AS amount
  FROM entries
  GROUP BY DATE(ts)
  UNION ALL
  SELECT '12' AS rk, 'Assets - ETH' AS item, DATE(ts) AS period, SUM(CASE WHEN account LIKE '12%' OR account LIKE '13%' THEN amount END) AS amount
  FROM entries
  GROUP BY DATE(ts)
  UNION ALL
  SELECT '2' AS rk, 'Liabilities' AS item, DATE(ts) AS period, -SUM(CASE WHEN account LIKE '2%' THEN amount END) AS amount
  FROM entries
  GROUP BY DATE(ts)
  UNION ALL
  SELECT '3' AS rk, 'Capital' AS item, DATE(ts) AS period, -SUM(CASE WHEN account LIKE '3%' THEN amount END) AS amount
  FROM entries
  GROUP BY DATE(ts)
),
balances AS (
  SELECT rk, item, period, SUM(amount) OVER (PARTITION BY item ORDER BY period ASC) AS balance
  FROM items
),
total_assets AS (
  SELECT period, SUM(balance) AS total_asset_balance
  FROM balances
  WHERE item IN ('Assets - USD', 'Assets - ETH')
  GROUP BY period
)

SELECT
    item,
    CAST(period AS TIMESTAMP) AS period,
    balance,
    balance/total_asset_balance AS normalized,
    total_asset_balance AS total_assets
FROM balances
JOIN total_assets USING (period)
WHERE period > CURRENT_DATE - INTERVAL '3' YEAR
ORDER BY
    period DESC,
    rk ASC;
