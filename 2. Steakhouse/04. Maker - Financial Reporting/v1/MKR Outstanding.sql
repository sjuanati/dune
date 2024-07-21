with exclusions AS (
    SELECT
      0x0000000000000000000000000000000000000000 AS wallet /* Mint/burn address */
    UNION ALL
    SELECT
      0xbe8e3e3618f7474f8cb1d074a26affef007e98fb /* MakerDAO DSPauseProxy */
    UNION ALL
    SELECT
      0x517f9dd285e75b599234f7221227339478d0fcc8 /* Uniswap V2 MKR/DAI pool mainly Maker owned */
  ),
  tokens AS (
    SELECT
      *
    FROM
      tokens.erc20
    WHERE
      blockchain = 'ethereum'
      AND contract_address = 0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2 /* MKR */
  ),
  delta AS (
    SELECT
      date_trunc('day', evt_block_time) AS period,
      contract_address,
      - CAST("value" AS INT256) / POWER(10, decimals) AS delta
    FROM
      erc20_ethereum.evt_Transfer
      INNER JOIN tokens USING (contract_address)
    WHERE
      "to" IN (
        SELECT
          wallet
        FROM
          exclusions
      )
    UNION ALL
    SELECT
      date_trunc('day', evt_block_time) AS period,
      contract_address,
      "value" / POWER(10, decimals) AS delta
    FROM
      erc20_ethereum.evt_Transfer
      INNER JOIN tokens USING (contract_address)
    WHERE
      "from" IN (
        SELECT
          wallet
        FROM
          exclusions
      )
    UNION ALL
    SELECT
      date_trunc('day', evt_block_time) AS period,
      contract_address,
      - CAST("wad" AS INT256)/ POWER(10, decimals) AS delta
    FROM
      maker_ethereum.MKR_evt_Burn
      INNER JOIN tokens USING (contract_address)
    UNION ALL
    SELECT
      date_trunc('day', evt_block_time) AS period,
      contract_address,
      "wad" / POWER(10, decimals) AS delta
    FROM
      maker_ethereum.MKR_evt_Mint
      INNER JOIN tokens USING (contract_address)
    UNION ALL
    SELECT
      period,
      contract_address,
      TRY_CAST(NULL AS DECIMAL) AS delta
    FROM
      tokens
      CROSS JOIN UNNEST (
        SEQUENCE(
          TRY_CAST('2019-11-01' AS DATE),
          CURRENT_DATE,
          INTERVAL '1' DAY
        )
      ) AS _u (period)
  ),
  group_by AS (
    SELECT
      period,
      contract_address,
      SUM(delta) AS delta
    FROM
      delta
    GROUP BY
      1,
      2
  ),
  balance AS (
    SELECT
      period,
      contract_address,
      SUM(delta) OVER (
        PARTITION BY
          contract_address
        ORDER BY
          period
      ) AS balance
    FROM
      group_by
  ),
  outstanding_token AS (
    SELECT
      period,
      symbol,
      balance
    FROM
      balance
      INNER JOIN tokens USING (contract_address)
    WHERE
      period >= CAST('2019-11-01' AS TIMESTAMP)
  ),
  starting_value AS (
    SELECT
      symbol,
      balance AS starting_balance
    FROM
      outstanding_token
    WHERE
      period = CAST('2019-11-01' AS TIMESTAMP)
  ),
  compute AS (
    SELECT
      period,
      symbol,
      balance,
      1000000 - balance AS burned_mkr,
      balance / LAG(balance, 365) OVER (
        ORDER BY
          period
      ) - 1 AS burn_1yr,
      balance / 1000000 AS remaining_mkr
    FROM
      outstanding_token AS o
      INNER JOIN starting_value USING (symbol)
  )
SELECT
  *,
  remaining_mkr * 100 AS remaining_mkr_pct
FROM
  compute
   -- and period >= current_date - interval '180' day
ORDER BY
  1 DESC,
  2