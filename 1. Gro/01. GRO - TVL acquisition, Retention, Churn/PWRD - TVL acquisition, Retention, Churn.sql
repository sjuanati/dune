/*
/// @title  TVL Acquisition, Retention & Churn for PWRD [Engine v2 Dune SQL]
/// @kpi    - New TVL: Deposits into the protocol during period by wallets that had never deposited into the protocol before
///         - Renewed TVL: Deposits into the protocol during period by wallets that had deposited into the protocol before
///         - Churned TVL: Withdrawals from the protocol during this period
///         - Retained: TVL in the protocol during the previous time period deducting churned TVL from this period
/// @dev    - TVL based on deposit & withdrawal events, therefore excluding rebasing / returns
///         - After G2 deployment, withdrawalHandler is still active for Argent users
*/

-- version 4.0 [Migrated to Dune SQL & included G2]
WITH
    -- Time period for the Dashboard (must be a week date, i.e.: 2021-05-24, 2021-05-31...)
    period AS (
        SELECT week
        FROM unnest(sequence(date('2021-05-24'), date_trunc('week', CURRENT_DATE), interval '7' day)) AS t(week)
    ),
    -- Deposits & withdrawals
    pwrd AS (
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS week,
               CAST(usdAmount AS DOUBLE) / 1e18 AS amount,
               'deposit' AS type,
               user AS wallet
          FROM gro_ethereum.DepositHandler_evt_LogNewDeposit
         WHERE pwrd = true
        UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS week,
               CAST(returnUsd AS DOUBLE) / 1e18 AS amount,
               'withdrawal' AS type,
               user AS wallet
          FROM gro_ethereum.WithdrawHandler_evt_LogNewWithdrawal
         WHERE pwrd = true
         UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS week,
               CAST(usdAmount AS DOUBLE) / 1e18 AS amount,
               'deposit' AS type,
               user AS wallet
          FROM gro_ethereum.DepositHandlerPrev1_evt_LogNewDeposit
         WHERE pwrd = true
        UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS week,
               CAST(returnUsd AS DOUBLE) / 1e18 AS amount,
               'withdrawal' AS type,
               user AS wallet
          FROM gro_ethereum.WithdrawHandlerPrev1_evt_LogNewWithdrawal
         WHERE pwrd = true
         UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS week,
               CAST(usdAmount AS DOUBLE) / 1e18 AS amount,
               'deposit' AS type,
               user AS wallet
          FROM gro_ethereum.DepositHandlerPrev2_evt_LogNewDeposit
         WHERE pwrd = true
        UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS week,
               CAST(returnUsd AS DOUBLE) / 1e18 AS amount,
               'withdrawal' AS type,
               user AS wallet
          FROM gro_ethereum.WithdrawHandlerPrev2_evt_LogNewWithdrawal
         WHERE pwrd = true
         UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS week,
               CAST(calcAmount AS DOUBLE) / 1e18 AS amount,
               'deposit' AS type,
               "sender" AS wallet
          FROM gro_ethereum.GRouter_evt_LogDeposit
         WHERE tranche = true
         UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS week,
               CASE WHEN CAST(tokenIndex AS INTEGER) = 0 
                    THEN CAST(calcAmount AS DOUBLE) / 1e18
                    ELSE CAST(calcAmount AS DOUBLE) / 1e6
                END AS amount,
               'withdrawal' AS type,
               "sender" AS wallet
          FROM gro_ethereum.GRouter_evt_LogWithdrawal
         WHERE tranche = true
    ),
    -- Sum of deposits & withdrawals by week & wallet
    pwrd_acc AS (
        SELECT week, 
               wallet, 
               SUM(CASE WHEN type = 'deposit' THEN pwrd.amount END) as deposit,
               SUM(CASE WHEN type = 'withdrawal' THEN pwrd.amount END) as withdraw
          FROM pwrd
        GROUP BY 1,2
    ),
    -- Calculation of
    -- a) New TVL (deposits into the protocol during this period by wallets that had never deposited into the protocol before)
    -- b) Renewed TVL (deposits into the protocol during this period by wallets that had deposited into the protocol before)
    -- c) Churned TVL (withdrawals from the protocol during this period)
    kpis AS (
        SELECT week,
               sum(new_tvl) as new_tvl,
               sum(renewed_tvl) as renewed_tvl,
               sum(churned_tvl) as churned_tvl
         FROM (
            SELECT week,
                wallet,
                deposit,
                withdraw,
                CASE
                    WHEN COALESCE(deposit, 0) > 0 
                     AND - COALESCE(deposit, 0) 
                         + COALESCE(withdraw, 0)
                         + SUM(COALESCE(deposit, 0) - COALESCE(withdraw, 0)) OVER (PARTITION BY wallet ORDER BY week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) <= 0
                    THEN deposit
                    ELSE 0
                END AS new_tvl,
                CASE
                    WHEN COALESCE(deposit, 0) > 0 
                     AND - COALESCE(deposit, 0) 
                         + COALESCE(withdraw, 0)
                         + SUM(COALESCE(deposit, 0) - COALESCE(withdraw, 0)) OVER (PARTITION BY wallet ORDER BY week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) > 0
                    THEN deposit
                    ELSE 0
                END AS renewed_tvl,
                COALESCE(withdraw, 0) as churned_tvl
            FROM pwrd_acc
           GROUP BY 1,2,3,4
         ) new
         GROUP BY 1
    ),
    -- Calculation of retained TVL (TVL in the protocol during the previous time period deducting churned TVL from this period)
    retained_tvl AS (
        SELECT week,
           sum(retained_tvl) as retained_tvl
         FROM (
            SELECT period.week,
                  -COALESCE(new_tvl,0)
                  -COALESCE(renewed_tvl, 0)
                  +SUM(COALESCE(new_tvl, 0) + COALESCE(renewed_tvl, 0) - COALESCE(churned_tvl, 0)) OVER (ORDER BY period.week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) as retained_tvl
            FROM period
            LEFT JOIN kpis
                ON period.week = kpis.week
        ) ret
         GROUP BY 1
    )

SELECT 
    SUBSTR(CAST(p.week AS varchar), 1, 10) AS week,
    kpis.new_tvl AS "New TVL",
    kpis.renewed_tvl AS "Renewed TVL",
    -kpis.churned_tvl AS "Churned TVL",
    rt.retained_tvl AS "Retained TVL",
    SUM(COALESCE(kpis.new_tvl, 0) + COALESCE(kpis.renewed_tvl, 0) - COALESCE(kpis.churned_tvl, 0)) OVER (ORDER BY p.week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) AS "Total TVL"
FROM period p
LEFT JOIN kpis
    ON p.week = kpis.week
LEFT JOIN retained_tvl rt
    ON p.week = rt.week
ORDER BY 1 DESC


