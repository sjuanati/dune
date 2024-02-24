/*
/// @title    PWRD Hodlers
/// @Version  4.0 [Migrated to Engine v2 Dune SQL & included G2]
/// @kpi    - Number of unique wallets with PWRD > $30 (or as defined per the balance parameter)
/// @param  - USD Balance: minimum balance per wallet
/// @dev    - Wallet count based on deposit & withdrawal handler events & GRouter events
*/

WITH
    -- Deposits & withdrawals for PWRD (incl. all three versions of handlers)
    pwrd AS (
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               CAST(usdAmount AS DOUBLE) / 1e18 AS "amount",
               'deposit' AS "type",
               user AS "wallet"
          FROM gro_ethereum.DepositHandler_evt_LogNewDeposit
         WHERE pwrd = true
        UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               CAST(returnUsd AS DOUBLE) / 1e18 AS "amount",
               'withdrawal' AS "type",
               user AS wallet
          FROM gro_ethereum.WithdrawHandler_evt_LogNewWithdrawal
         WHERE pwrd = true
         UNION ALL
                 SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               CAST(usdAmount AS DOUBLE) / 1e18 AS "amount",
               'deposit' AS "type",
               user AS "wallet"
          FROM gro_ethereum.DepositHandlerPrev1_evt_LogNewDeposit
         WHERE pwrd = true
        UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               CAST(returnUsd AS DOUBLE) / 1e18 AS "amount",
               'withdrawal' AS "type",
               user AS wallet
          FROM gro_ethereum.WithdrawHandlerPrev1_evt_LogNewWithdrawal
         WHERE pwrd = true
         UNION ALL
                 SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               CAST(usdAmount AS DOUBLE) / 1e18 AS "amount",
               'deposit' AS "type",
               user AS "wallet"
          FROM gro_ethereum.DepositHandlerPrev2_evt_LogNewDeposit
         WHERE pwrd = true
        UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               CAST(returnUsd AS DOUBLE) / 1e18 AS "amount",
               'withdrawal' AS "type",
               user AS "wallet"
          FROM gro_ethereum.WithdrawHandlerPrev2_evt_LogNewWithdrawal
         WHERE pwrd = true
         UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               CAST(calcAmount AS DOUBLE) / 1e18 AS "amount",
               'deposit' AS "type",
               "sender" AS "wallet"
          FROM gro_ethereum.GRouter_evt_LogDeposit
         WHERE tranche = true
         UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               CAST(calcAmount AS DOUBLE) / 1e6 AS "amount",
               'withdrawal' AS "type",
               "sender" AS "wallet"
          FROM gro_ethereum.GRouter_evt_LogWithdrawal
         WHERE tranche = true
    ),
    -- Total PWRD balance per user
    pwrd_acc AS (
        SELECT wallet, 
               SUM(CASE WHEN type = 'deposit' THEN amount ELSE -amount END) amount
          FROM pwrd
        GROUP BY 1
    ),
    -- Number of wallets with PWRD balance > [PWRD - USD Balance] parameter
    hodlers AS (
        SELECT CASE WHEN amount > CAST('{{PWRD - USD balance}}' AS double) THEN 1 ELSE 0
               END as hodler_addr
        FROM pwrd_acc
    )

SELECT SUM(hodler_addr) as "PWRD Hodlers" from hodlers

