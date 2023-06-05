/*
/// @title  GRO - TVL v4 [Engine V2 - Dune SQL]
/// @kpi    - Total TVL of GVT & PWRD
/// @dev    - PWRD: USD amounts based on deposit & withdrawal handler events, therefore excluding returns & rebasing (pwrd)
/// @dev    - GVT: USD amounts based on transfer amount * price deduced from PNL events
*/

WITH
    -- Deposits & withdrawals (incl. all three versions of deposit/withdrawal handlers + GRouter for G2)
    pwrd AS (
        SELECT date_trunc('week', evt_block_time) AS week,
               CAST(usdAmount AS DOUBLE) / 1e18 AS amount,
               'deposit' AS side,
               "user" AS wallet,
               "pwrd" AS pwrd
          FROM gro_ethereum.DepositHandler_evt_LogNewDeposit
        UNION ALL
        SELECT date_trunc('week', evt_block_time) AS week,
               CAST(returnUsd AS DOUBLE) / 1e18 AS amount,
               'withdrawal' AS  side,
               "user" AS wallet,
               "pwrd" AS pwrd
          FROM gro_ethereum.WithdrawHandler_evt_LogNewWithdrawal
         UNION ALL
        SELECT date_trunc('week', evt_block_time) AS week,
               CAST(usdAmount AS DOUBLE) / 1e18 AS amount,
               'deposit' AS side,
               "user" AS wallet,
               "pwrd" AS pwrd
          FROM gro_ethereum.DepositHandlerPrev1_evt_LogNewDeposit
        UNION ALL
        SELECT date_trunc('week', evt_block_time) AS week,
               CAST(returnUsd AS DOUBLE) / 1e18 AS amount,
               'withdrawal' AS side,
               "user" AS wallet,
               "pwrd" AS pwrd
          FROM gro_ethereum.WithdrawHandlerPrev1_evt_LogNewWithdrawal
         UNION ALL
        SELECT date_trunc('week', evt_block_time) AS week,
               CAST(usdAmount AS DOUBLE) / 1e18 AS amount,
               'deposit' AS side,
               "user" AS wallet,
               "pwrd" AS pwrd
          FROM gro_ethereum.DepositHandlerPrev2_evt_LogNewDeposit
        UNION ALL
        SELECT date_trunc('week', evt_block_time) AS week,
               CAST(returnUsd AS DOUBLE) / 1e18 AS amount,
               'withdrawal' AS side,
               "user" AS wallet,
               "pwrd" AS pwrd
          FROM gro_ethereum.WithdrawHandlerPrev2_evt_LogNewWithdrawal
         UNION ALL
        SELECT date_trunc('week', evt_block_time) AS week,
               CAST(calcAmount AS DOUBLE) / 1e18 AS amount,
               'deposit' AS side,
               "sender" AS wallet,
               "tranche" AS pwrd
          FROM gro_ethereum.GRouter_evt_LogDeposit
         UNION ALL
        SELECT date_trunc('week', evt_block_time) AS week,
               CAST(calcAmount AS DOUBLE) / 1e18 AS amount,
               'withdrawal' AS side,
               "sender" AS wallet,
               "tranche" AS pwrd
          FROM gro_ethereum.GRouter_evt_LogWithdrawal
    ),
    -- Total GVT supply based on ERC20 transfers
    gvt_total_supply AS (
        SELECT
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN CAST(value AS DOUBLE)
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN - CAST(value AS DOUBLE)
                END
            ) / 1e18 AS total_supply
        FROM erc20_ethereum.evt_Transfer tr
        WHERE contract_address = 0x3ADb04E127b9C0a5D36094125669d4603AC52a0c
    ),
    -- Total GVT assets based on PnL events
    gvt_total_assets AS (
        SELECT CAST(balances[1] AS DOUBLE) / 1e18 as gvt_assets
        FROM gro_ethereum.GTranche_evt_LogNewTrancheBalance
        ORDER BY evt_block_number DESC
        LIMIT 1
    ),
    -- PWRD TVL
    pwrd_tvl AS (
        SELECT SUM(
            CASE WHEN pwrd = true THEN 
                 CASE WHEN side = 'deposit' THEN amount 
                      ELSE -amount 
                 END 
            ELSE 0 END
            ) AS pwrd_tvl
        FROM pwrd
    ),
    -- GVT TVL
    gvt_tvl AS (
        SELECT total_supply * (gvt_assets / total_supply) AS gvt_tvl
        FROM gvt_total_supply,
             gvt_total_assets
    )

SELECT pwrd_tvl + gvt_tvl AS "Total TVL",
       pwrd_tvl AS "PWRD TVL",
       gvt_tvl AS "GVT TVL"
FROM gvt_tvl,
     pwrd_tvl
