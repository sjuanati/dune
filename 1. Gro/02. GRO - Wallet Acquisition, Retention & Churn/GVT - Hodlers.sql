/*
/// @title    GVT Hodlers
/// @Version  4.0 [Migrated to Engine v2 Dune SQL & included G2]
/// @kpi    - Number of unique wallets with GVT > $30 (or as defined per the balance parameter)
/// @param  - USD Balance: minimum balance per wallet
/// @dev    - Wallet count based on deposit & withdrawal handler events & GRouter events
*/

WITH
    -- Deposits & withdrawals (incl. all three versions of handlers & GRouter)
    gvt AS (
        SELECT evt_block_time AS time,
               date_trunc('week', evt_block_time) AS week,
               CAST(usdAmount AS DOUBLE) / 1e18 AS amount,
               'deposit' AS type,
               user AS wallet
          FROM gro_ethereum.DepositHandler_evt_LogNewDeposit
         WHERE pwrd = false
        UNION ALL
        SELECT evt_block_time AS time,
               date_trunc('week', evt_block_time) AS week,
               CAST(returnUsd AS DOUBLE) / 1e18 AS amount,
               'withdrawal' AS type,
               user AS wallet
          FROM gro_ethereum.WithdrawHandler_evt_LogNewWithdrawal
         WHERE pwrd = false
         UNION ALL
                 SELECT evt_block_time AS time,
               date_trunc('week', evt_block_time) AS week,
               CAST(usdAmount AS DOUBLE) / 1e18 AS amount,
               'deposit' AS type,
               user AS wallet
          FROM gro_ethereum.DepositHandlerPrev1_evt_LogNewDeposit
         WHERE pwrd = false
        UNION ALL
        SELECT evt_block_time AS time,
               date_trunc('week', evt_block_time) AS week,
               CAST(returnUsd AS DOUBLE) / 1e18 AS amount,
               'withdrawal' AS type,
               user AS wallet
          FROM gro_ethereum.WithdrawHandlerPrev1_evt_LogNewWithdrawal
         WHERE pwrd = false
         UNION ALL
                 SELECT evt_block_time AS time,
               date_trunc('week', evt_block_time) AS week,
               CAST(usdAmount AS DOUBLE) / 1e18 AS amount,
               'deposit' AS type,
               user AS wallet
          FROM gro_ethereum.DepositHandlerPrev2_evt_LogNewDeposit
         WHERE pwrd = false
        UNION ALL
        SELECT evt_block_time AS time,
               date_trunc('week', evt_block_time) AS week,
               CAST(returnUsd AS DOUBLE) / 1e18 AS amount,
               'withdrawal' AS type,
               user AS wallet
          FROM gro_ethereum.WithdrawHandlerPrev2_evt_LogNewWithdrawal
         WHERE pwrd = false
         UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               CAST(calcAmount AS DOUBLE) / 1e18 AS "amount",
               'deposit' AS "type",
               "sender" AS "wallet"
          FROM gro_ethereum.GRouter_evt_LogDeposit
         WHERE tranche = false
         UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               CAST(calcAmount AS DOUBLE) / 1e18 AS "amount",
               'withdrawal' AS "type",
               "sender" AS "wallet"
          FROM gro_ethereum.GRouter_evt_LogWithdrawal
         WHERE tranche = false
    ),
    -- Total GVT balance per user
    gvt_acc AS (
        SELECT wallet, 
               SUM(CASE WHEN type = 'deposit' THEN amount ELSE -amount END) amount
          FROM gvt
        GROUP BY 1
    ),
    -- Number of wallets with GVT balance > [GVT - USD Balance] parameter
    hodlers AS (
        SELECT CASE WHEN amount > CAST('{{GVT - USD balance}}' AS double) THEN 1 ELSE 0
               END as hodler_addr
        FROM gvt_acc
    )

SELECT SUM(hodler_addr) as "GVT Hodlers" from hodlers
