WITH
  addresses AS (
    SELECT
      0xbf72da2bd84c5170618fbe5914b0eca9638d5eb5 AS wallet,
      'Lending' AS wallet_type,
      'MakerDAO' AS wallet_name /* WBTC-A */
    UNION ALL
    SELECT
      0xfA8c996e158B80D77FbD0082BB437556A65B96E0 AS wallet,
      'Lending' AS wallet_type,
      'MakerDAO' AS wallet_name /* WBTC-B */
    UNION ALL
    SELECT
      0x7f62f9592b823331E012D3c5DdF2A7714CfB9de2 AS wallet,
      'Lending' AS wallet_type,
      'MakerDAO' AS wallet_name /* WBTC-C */
    UNION ALL
    SELECT
      0xccF4429DB6322D5C611ee964527D42E5d685DD6a AS wallet,
      'Lending' AS wallet_type,
      'Compound' AS wallet_name
    UNION ALL
    SELECT
      0xc11b1268c1a384e55c48c2391d8d480264a3a7f4 AS wallet,
      'Lending' AS wallet_type,
      'Compound' AS wallet_name
    UNION ALL
    SELECT
      0x9ff58f4fFB29fA2266Ab25e75e2A8b3503311656 AS wallet,
      'Lending' AS wallet_type,
      'Aave' AS wallet_name
    UNION ALL
    SELECT
      0x3dfd23a6c5e8bbcfc9581d2e864a68feb6a076d3 AS wallet,
      'Lending' AS wallet_type,
      'Aave' AS wallet_name
  ),
  deltas AS (
    SELECT
      "to" AS wallet,
      TRY_CAST("evt_block_time" AS DATE) AS dt,
      CAST(value AS INT256) AS delta
    FROM
      erc20_ethereum.evt_Transfer
      INNER JOIN addresses ON (wallet = "to")
    WHERE
      contract_address = 0x2260fac5e5542a773aa44fbcfedf7c193bc2c599 /* WBTC */
    UNION ALL
    SELECT
      "from" AS wallet,
      TRY_CAST("evt_block_time" AS DATE) AS dt,
      - CAST(value AS INT256) AS delta
    FROM
      erc20_ethereum.evt_Transfer
      INNER JOIN addresses ON (wallet = "from")
    WHERE
      contract_address = 0x2260fac5e5542a773aa44fbcfedf7c193bc2c599 /* WBTC */
  ),
  all_holders AS (
    SELECT
      wallet,
      MIN(dt) AS starting_date
    FROM
      deltas
    GROUP BY
      1
  ),
  filler AS (
    SELECT
      wallet,
      dt,
      CAST(0 AS INT256) AS delta
    FROM
      all_holders
      CROSS JOIN UNNEST (
        SEQUENCE(starting_date, CURRENT_DATE, INTERVAL '1' day)
      ) AS _u (dt)
  ),
  merged AS (
    SELECT
      *
    FROM
      deltas
    UNION ALL
    SELECT
      *
    FROM
      filler
  ),
  grouped AS (
    SELECT
      wallet_name AS wallet,
      dt,
      SUM(delta) AS delta
    FROM
      merged
      INNER JOIN addresses USING (wallet)
    WHERE
      wallet <> 0x0000000000000000000000000000000000000000
    GROUP BY
      1,
      2
  ),
  balances AS (
    SELECT
      wallet,
      dt,
      SUM(delta) OVER (
        PARTITION BY
          wallet
        ORDER BY
          dt
      ) / POWER(10, 8) AS balance
    FROM
      grouped
  )
SELECT
  wallet,
  TRY_CAST(dt AS TIMESTAMP) AS dt,
  balance,
  balance / SUM(balance) OVER (
    PARTITION BY
      dt
  ) AS market_share,
  balance / SUM(balance) OVER (
    PARTITION BY
      dt
  ) * 100 AS market_share_100
FROM
  balances
WHERE
  dt >= CURRENT_DATE - INTERVAL '365' DAY
ORDER BY
  2 DESC,
  CASE
    WHEN wallet = 'MakerDAO' THEN 1
    ELSE 99
  END