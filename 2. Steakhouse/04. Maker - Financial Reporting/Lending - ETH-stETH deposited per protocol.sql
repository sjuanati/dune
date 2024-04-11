WITH
  addresses AS (
    SELECT
      0x2F0b23f53734252Bda2277357e97e1517d6B042A AS wallet,
      'Lending' AS wallet_type,
      'MakerDAO' AS wallet_name /* ETH-A */
    UNION ALL
    SELECT
      0x08638eF1A205bE6762A8b935F5da9b700Cf7322c AS wallet,
      'Lending' AS wallet_type,
      'MakerDAO' AS wallet_name /* ETH-B */
    UNION ALL
    SELECT
      0xF04a5cC80B1E94C69B48f5ee68a08CD2F09A7c3E AS wallet,
      'Lending' AS wallet_type,
      'MakerDAO' AS wallet_name /* ETH-C */
    UNION ALL
    SELECT
      0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5 AS wallet,
      'Lending' AS wallet_type,
      'Compound' AS wallet_name /* vanilla ETH dealt differently */
    UNION ALL
    SELECT
      0x030ba81f1c18d280636f32af80b9aad02cf0854e AS wallet,
      'Lending' AS wallet_type,
      'Aave' AS wallet_name /* v2 */
    UNION ALL
    SELECT
      0x3a3a65aab0dd2a17e3f1947ba16138cd37d08c04 AS wallet,
      'Lending' AS wallet_type,
      'Aave' AS wallet_name /* v1 */
    UNION ALL
    SELECT
      0xDf9Eb223bAFBE5c5271415C75aeCD68C21fE3D7F AS wallet,
      'Lending' AS wallet_type,
      'Liquity' AS wallet_name
    UNION ALL
    SELECT
      0x10CD5fbe1b404B7E19Ef964B63939907bdaf42E2 AS wallet,
      'Lending' AS wallet_type,
      'MakerDAO' AS wallet_name /* WSTETH-A, transacts in wsteth */
    UNION ALL
    SELECT
      0x1982b2F5814301d4e9a8b0201555376e62F82428 AS wallet,
      'Lending' AS wallet_type,
      'Aave' AS wallet_name /* aSTETH, transacts in steth */
  ),
  lido_wrap_ratios AS (
    SELECT
      *
    FROM
      (
        SELECT
          call_block_time AS ts,
          CAST(output_0 AS DOUBLE) / "_wstETHAmount" AS wrap_ratio
        FROM
          lido_ethereum.WstETH_call_getStETHByWstETH
        WHERE
          call_success
          AND "_wstETHAmount" > CAST(0 AS UINT256)
        UNION ALL
        SELECT
          call_block_time AS ts,
          CAST("_stETHAmount" AS DOUBLE) / output_0 AS wrap_ratio
        FROM
          lido_ethereum.WstETH_call_getWstETHByStETH
        WHERE
          call_success
          AND output_0 > CAST(0 AS UINT256)
      ) AS sub
    GROUP BY
      1,
      2
  ),
  deltas /* weth, steth, wsteth then eth (on compound+liquity) */ AS (
    SELECT
      "to" AS wallet,
      TRY_CAST("evt_block_time" AS DATE) AS dt,
      CAST(value AS DOUBLE) AS delta
    FROM
      erc20_ethereum.evt_Transfer
      INNER JOIN addresses ON (wallet = "to")
    WHERE
      contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 /* WETH */
    UNION ALL
    SELECT
      "from" AS wallet,
      TRY_CAST("evt_block_time" AS DATE) AS dt,
      - CAST(value AS DOUBLE) AS delta
    FROM
      erc20_ethereum.evt_Transfer
      INNER JOIN addresses ON (wallet = "from")
    WHERE
      contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 /* WETH */
    UNION ALL
    SELECT
      contract_address AS wallet,
      TRY_CAST("evt_block_time" AS DATE) AS dt,
      CAST(value AS DOUBLE) AS delta
    FROM
      erc20_ethereum.evt_Transfer
    WHERE
      contract_address = 0x3a3a65aab0dd2a17e3f1947ba16138cd37d08c04 /* aETH */
      AND "from" = 0x0000000000000000000000000000000000000000
    UNION ALL
    SELECT
      contract_address AS wallet,
      TRY_CAST("evt_block_time" AS DATE) AS dt,
      - CAST(value AS DOUBLE) AS delta
    FROM
      erc20_ethereum.evt_Transfer
    WHERE
      contract_address = 0x3a3a65aab0dd2a17e3f1947ba16138cd37d08c04 /* aETH */
      AND "to" = 0x0000000000000000000000000000000000000000
    UNION ALL
    SELECT
      "to" AS wallet,
      TRY_CAST("evt_block_time" AS DATE) AS dt,
      CAST(value AS DOUBLE) AS delta
    FROM
      erc20_ethereum.evt_Transfer
      INNER JOIN addresses ON (wallet = "to")
    WHERE
      contract_address = 0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0 /* wsteth */
    UNION ALL
    SELECT
      "from" AS wallet,
      TRY_CAST("evt_block_time" AS DATE) AS dt,
      - CAST(value AS DOUBLE) AS delta
    FROM
      erc20_ethereum.evt_Transfer
      INNER JOIN addresses ON (wallet = "from")
    WHERE
      contract_address = 0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0 /* wsteth */
    UNION ALL
    SELECT
      "to" AS wallet,
      TRY_CAST("evt_block_time" AS DATE) AS dt,
      CAST(value AS DOUBLE) AS delta
    FROM
      erc20_ethereum.evt_Transfer
      INNER JOIN addresses ON (wallet = "to")
    WHERE
      contract_address = 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84 /* steth */
    UNION ALL
    SELECT
      "from" AS wallet,
      TRY_CAST("evt_block_time" AS DATE) AS dt,
      - CAST(value AS DOUBLE) AS delta
    FROM
      erc20_ethereum.evt_Transfer
      INNER JOIN addresses ON (wallet = "from")
    WHERE
      contract_address = 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84 /* steth */
    UNION ALL
    SELECT
      "to" AS wallet,
      TRY_CAST("block_time" AS DATE) AS dt,
      CAST(value AS DOUBLE) AS delta
    FROM
      ethereum."traces"
    WHERE
      "to" IN (
        0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5,
        0xDf9Eb223bAFBE5c5271415C75aeCD68C21fE3D7F
      )
      AND tx_success
      AND success
    UNION ALL
    SELECT
      "from" AS wallet,
      TRY_CAST("block_time" AS DATE) AS dt,
      - CAST(value AS DOUBLE) AS delta
    FROM
      ethereum."traces"
    WHERE
      "from" IN (
        0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5,
        0xDf9Eb223bAFBE5c5271415C75aeCD68C21fE3D7F
      )
      AND tx_success
      AND success
  )
  /*  union all
  -- Compound by contracts call
  -- liquidations are already included in these base tables, so would be double counted if we unioned the liquidations table as well
  select contract_address as wallet, evt_block_time::date as dt, "mintAmount" as delta
  from compound_v2."cEther_evt_Mint"
  union all
  select contract_address as wallet, evt_block_time::date as dt, -"redeemAmount" as delta
  from compound_v2."cEther_evt_Redeem"
  union all
  select contract_address as wallet, evt_block_time::date as dt, -"borrowAmount" as delta
  from compound_v2."cEther_evt_Borrow"
  union all
  select contract_address as wallet, evt_block_time::date as dt, "repayAmount" as delta
  from compound_v2."cEther_evt_RepayBorrow" */
,
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
      0 AS delta
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
  grouped_wrapped AS (
    SELECT
      wallet_name AS wallet,
      dt,
      SUM(delta) AS delta
    FROM
      merged
      INNER JOIN addresses USING (wallet)
    WHERE
      wallet = 0x10CD5fbe1b404B7E19Ef964B63939907bdaf42E2
    GROUP BY
      1,
      2
  ),
  balances_wrapped AS (
    SELECT
      wallet,
      dt,
      SUM(delta) OVER (
        PARTITION BY
          wallet
        ORDER BY
          dt
      ) / POWER(10, 18) AS balance
    FROM
      grouped_wrapped
  ),
  balances_unwrapped AS (
    SELECT
      wallet,
      dt,
      equivalent_steth_balance
    FROM
      (
        SELECT
          balances_wrapped.*,
          ts,
          wrap_ratio,
          balances_wrapped.balance / COALESCE(wrap_ratio, 1) AS equivalent_steth_balance,
          /* does not divide when the contract address is not the wrapped token address */ ROW_NUMBER() OVER (
            PARTITION BY
              wallet,
              dt
            ORDER BY
              ts DESC
          ) AS rn
        FROM
          balances_wrapped
          LEFT JOIN lido_wrap_ratios ON balances_wrapped.dt >= DATE(lido_wrap_ratios.ts) /* getting the most recent wrap ratio called on or before the date of said balance */
      ) AS sub
    WHERE
      rn = 1
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
      NOT wallet IN (
        0x0000000000000000000000000000000000000000,
        0x10CD5fbe1b404B7E19Ef964B63939907bdaf42E2
      )
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
      ) / POWER(10, 18) AS balance
    FROM
      grouped
  ),
  balances_final AS (
    SELECT
      wallet,
      dt,
      balance + COALESCE(equivalent_steth_balance, 0) AS balance
    FROM
      balances
      LEFT JOIN balances_unwrapped USING (wallet, dt)
  )
SELECT
  wallet,
  TRY_CAST(
    dt AS TIMESTAMP
    WITH
      TIME ZONE
  ) AS dt,
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
  balances_final
WHERE
  dt >= CURRENT_DATE - INTERVAL '365' DAY
ORDER BY
  2 DESC,
  CASE
    WHEN wallet = 'MakerDAO' THEN 1
    ELSE 99
  END