/*
/// @title  GRO - TVL v3
/// @kpi    - Total TVL of GVT & PWRD
/// @dev    - PWRD: USD amounts based on deposit & withdrawal handler events, therefore excluding returns & rebasing (pwrd)
/// @dev    - GVT: USD amounts based on transfer amount * price deduced from PNL events
*/

WITH
    -- Deposits & withdrawals (incl. all three versions of handlers)
    pwrd AS (
        SELECT date_trunc('week', evt_block_time) AS "week",
               "usdAmount" / 1e18 AS "amount",
               'deposit' AS "type",
               "user" AS "wallet",
               "pwrd" AS "pwrd"
          FROM gro."DepositHandler_evt_LogNewDeposit"
        UNION ALL
        SELECT date_trunc('week', evt_block_time)::date AS "week",
               "returnUsd" / 1e18 AS "amount",
               'withdrawal' AS "type",
               "user" AS "wallet",
               "pwrd" AS "pwrd"
          FROM gro."WithdrawHandler_evt_LogNewWithdrawal"
         UNION ALL
        SELECT date_trunc('week', evt_block_time) AS "week",
               "usdAmount" / 1e18 AS "amount",
               'deposit' AS "type",
               "user" AS "wallet",
               "pwrd" AS "pwrd"
          FROM gro."DepositHandlerPrev1_evt_LogNewDeposit"
        UNION ALL
        SELECT date_trunc('week', evt_block_time)::date AS "week",
               "returnUsd" / 1e18 AS "amount",
               'withdrawal' AS "type",
               "user" AS "wallet",
               "pwrd" AS "pwrd"
          FROM gro."WithdrawHandlerPrev1_evt_LogNewWithdrawal"
         UNION ALL
        SELECT date_trunc('week', evt_block_time) AS "week",
               "usdAmount" / 1e18 AS "amount",
               'deposit' AS "type",
               "user" AS "wallet",
               "pwrd" AS "pwrd"
          FROM gro."DepositHandlerPrev2_evt_LogNewDeposit"
        UNION ALL
        SELECT date_trunc('week', evt_block_time)::date AS "week",
               "returnUsd" / 1e18 AS "amount",
               'withdrawal' AS "type",
               "user" AS "wallet",
               "pwrd" AS "pwrd"
          FROM gro."WithdrawHandlerPrev2_evt_LogNewWithdrawal"
    ),
    -- Total GVT supply based on ERC20 transfers
    gvt_total_supply AS (
        SELECT
            SUM(
                CASE
                    WHEN "from" = '\x0000000000000000000000000000000000000000' THEN value
                    WHEN "to" = '\x0000000000000000000000000000000000000000' THEN -value
                END
            ) / 1e18 AS "total_supply"
        FROM erc20."ERC20_evt_Transfer" tr
        WHERE contract_address = '\x3ADb04E127b9C0a5D36094125669d4603AC52a0c'
    ),
    -- Total GVT assets based on PnL events
    gvt_total_assets AS (
        SELECT pnl."afterGvtAssets" / 1e18 as "gvt_assets"
        FROM gro."PnL_evt_LogPnLExecution" pnl
        ORDER BY evt_block_number DESC
        LIMIT 1
    ),
    -- PWRD TVL
    pwrd_tvl AS (
        SELECT SUM(CASE WHEN pwrd = true THEN CASE WHEN type = 'deposit' THEN amount ELSE -amount END ELSE 0 END) AS "pwrd_tvl"
        FROM pwrd
    ),
    -- GVT TVL
    gvt_tvl AS (
        SELECT total_supply * (gvt_assets / total_supply) as "gvt_tvl"
        FROM gvt_total_supply,
             gvt_total_assets
    )

SELECT pwrd_tvl + gvt_tvl AS "Total TVL",
       pwrd_tvl AS "PWRD TVL",
       gvt_tvl AS "GVT TVL"
FROM gvt_tvl,
     pwrd_tvl
