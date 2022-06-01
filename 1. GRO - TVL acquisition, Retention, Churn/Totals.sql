/*
/// @title  GRO - TVL
/// @kpi    - Total TVL of GVT & PWRD
/// @dev    - USD amounts based on deposit & withdrawal handler events, therefore excluding returns & rebasing (pwrd)
*/

WITH
    -- Deposits & withdrawals (incl. all three versions of handlers)
    handler AS (
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               "usdAmount" / 1e18 AS "amount",
               'deposit' AS "type",
               "user" AS "wallet",
               "pwrd" AS "pwrd"
          FROM gro."DepositHandler_evt_LogNewDeposit"
        UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time)::date AS "week",
               "returnUsd" / 1e18 AS "amount",
               'withdrawal' AS "type",
               "user" AS "wallet",
               "pwrd" AS "pwrd"
          FROM gro."WithdrawHandler_evt_LogNewWithdrawal"
         UNION ALL
                 SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               "usdAmount" / 1e18 AS "amount",
               'deposit' AS "type",
               "user" AS "wallet",
               "pwrd" AS "pwrd"
          FROM gro."DepositHandlerPrev1_evt_LogNewDeposit"
        UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time)::date AS "week",
               "returnUsd" / 1e18 AS "amount",
               'withdrawal' AS "type",
               "user" AS "wallet",
               "pwrd" AS "pwrd"
          FROM gro."WithdrawHandlerPrev1_evt_LogNewWithdrawal"
         UNION ALL
                 SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               "usdAmount" / 1e18 AS "amount",
               'deposit' AS "type",
               "user" AS "wallet",
               "pwrd" AS "pwrd"
          FROM gro."DepositHandlerPrev2_evt_LogNewDeposit"
        UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time)::date AS "week",
               "returnUsd" / 1e18 AS "amount",
               'withdrawal' AS "type",
               "user" AS "wallet",
               "pwrd" AS "pwrd"
          FROM gro."WithdrawHandlerPrev2_evt_LogNewWithdrawal"
    )

SELECT SUM(CASE WHEN type = 'deposit' THEN amount ELSE -amount END) AS "Total TVL",
       SUM(CASE WHEN pwrd = true THEN CASE WHEN type = 'deposit' THEN amount ELSE -amount END ELSE 0 END) AS "PWRD TVL",
       SUM(CASE WHEN pwrd = false THEN CASE WHEN type = 'deposit' THEN amount ELSE -amount END ELSE 0 END) AS "GVT TVL"
FROM handler

