/*
-- @title: Maker - PSM stats
-- @description: Calculates daily PSM flow metrics (inflow, outflow, net change, fees), including those excluding RWA-related transactions
-- @author: Steakhouse Financial
-- @notes: N/A
-- @version:
    - 3.0 - 2024-04-10 - Added outflows excluding RWA: added field outflow_exl_rwa and updated change_excl_rwa
    - 2.0 - 2024-03-05 - Added inflows excluding RWA: added fields inflow_exl_rwa, change_excl_rwa, change_excl_rwa_7d_avg, change_excl_rwa_30d_avg
    - 1.0 - 2023-06-13 - Initial version
*/

WITH
  dates AS (
    SELECT
      date
    FROM
      UNNEST (
        SEQUENCE(
          TRY_CAST('2020-12-29' AS DATE),
          CURRENT_DATE,
          INTERVAL '1' day
        )
      ) AS _u (date)
  ),
  psms AS (
    SELECT DISTINCT
      "u" AS psm_address
    FROM
      maker_ethereum.VAT_call_frob
    WHERE
      REPLACE(FROM_UTF8(i),U&'\0000', '') LIKE 'PSM-%'
      AND call_success
  ),
  rwa_inflow_tx AS (
    select evt_tx_hash as tx
    from erc20_ethereum.evt_transfer
    where contract_address = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 -- usdc
    and date("evt_block_time") > date '2022-09-22'
    and "from" in (
        0x6B86bA08Bd7796464cEa758061Ac173D0268cf49, -- coinbase
        0xe08cb5E24862eA86328295D5E5c08972203C20D8, -- andromeda
        0x58f5e979eF74b60a9e5F955553ab8e0e65ba89c9  -- clydesdale
        )
    and "to" = 0x0A59649758aa4d66E25f08Dd01271e891fe52199 -- psm-usdc-a
  ),
  rwa_outflow_tx AS (
    select evt_tx_hash as tx
    from erc20_ethereum.evt_transfer
    where contract_address = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 -- usdc
    and date("evt_block_time") > date '2022-09-21'
    and "from" = 0x0A59649758aa4d66E25f08Dd01271e891fe52199 -- psm-usdc-a
    and "to" in (
        0x2E5F1f08EBC01d6136c95a40e19D4c64C0be772c, -- coinbase
        0xC3acf3B96E46Aa35dBD2aA3BD12D23c11295E774  -- clydesdale
        -- 0x... -- andromeda (TBC with the first outflow thru its conduit)
    )
  ),
  psm_tx AS (
    SELECT
      call_tx_hash AS tx,
      call_block_time,
      REPLACE(FROM_UTF8(i),U&'\0000', '') AS ilk,
      SUM(dink / CAST(POWER(10, 18) AS DOUBLE)) AS amount
    FROM
      maker_ethereum.VAT_call_frob
    WHERE
      REPLACE(FROM_UTF8(i),U&'\0000', '') LIKE 'PSM-%'
      AND call_success
    GROUP BY
      1,
      2,
      3
  ),
  tx_fees AS (
    SELECT
      call_tx_hash AS tx,
      SUM(rad / CAST(POWER(10, 45) AS DOUBLE)) AS fees
    FROM
      maker_ethereum.VAT_call_move
      INNER JOIN psm_tx ON tx = call_tx_hash
      INNER JOIN psms ON src = psm_address
    WHERE
      dst = 0xa950524441892a31ebddf91d3ceefa04bf454466
    GROUP BY
      1
  ),
  group_by AS (
    SELECT
      TRY_CAST(call_block_time AS DATE) AS date,
      SUM(GREATEST(amount, 0)) AS inflow,
      SUM(IF(rwa_in.tx IS NULL, GREATEST(amount, 0), 0)) AS inflow_exl_rwa,
      - SUM(LEAST(amount, 0)) AS outflow,
      - SUM(IF(rwa_out.tx IS NULL, LEAST(amount, 0), 0)) AS outflow_exl_rwa,
      SUM(ABS(amount)) AS turnover,
      SUM(amount) AS change,
      SUM(CASE WHEN rwa_in.tx IS NULL AND rwa_out.tx IS NULL THEN amount ELSE 0 END) AS change_excl_rwa,
      SUM(fees) AS fees
    FROM
      psm_tx psm
      LEFT JOIN tx_fees fee on psm.tx = fee.tx
      LEFT JOIN rwa_inflow_tx rwa_in on psm.tx = rwa_in.tx
      LEFT JOIN rwa_outflow_tx rwa_out on psm.tx = rwa_out.tx
    GROUP BY
      1
  )
SELECT
  *,
  AVG(change_excl_rwa) OVER (
    ORDER BY
      date ASC ROWS 6 PRECEDING
  ) AS change_excl_rwa_7d_avg,
  AVG(change_excl_rwa) OVER (
    ORDER BY
      date ASC ROWS 29 PRECEDING
  ) AS change_excl_rwa_30d_avg,
  SUM(change) OVER (
    ORDER BY
      date
  ) AS psm_balance,
  SUM(turnover) OVER (
    ORDER BY
      date
  ) AS lifetime_turnover,
  SUM(fees) OVER (
    ORDER BY
      date
  ) AS lifetime_fees
FROM
  dates
  LEFT OUTER JOIN group_by USING (date)
ORDER BY
  date DESC