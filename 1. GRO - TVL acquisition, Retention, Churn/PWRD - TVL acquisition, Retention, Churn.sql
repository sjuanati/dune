/*
/// @title  TVL Acquisition, Retention & Churn for PWRD
/// @kpi    - New TVL: Deposits into the protocol during period by wallets that had never deposited into the protocol before
///         - Renewed TVL: Deposits into the protocol during period by wallets that had deposited into the protocol before
///         - Churned TVL: Withdrawals from the protocol during this period
///         - Retained: TVL in the protocol during the previous time period deducting churned TVL from this period
/// @dev    - TVL based on deposit & withdrawal events, therefore excluding rebasing / returns
*/

-- version 2.0
WITH
    -- Time period for the Dashboard (must be a week date, i.e.: 2021-05-24, 2021-05-31...)
    period AS (
        SELECT generate_series('2021-05-24'::TIMESTAMP, date_trunc('week', NOW()), '1 week') AS "week"
    ),
    -- Deposits & withdrawals
    pwrd AS (
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               "usdAmount" / 1e18 AS "amount",
               'deposit' AS "type",
               "user" AS "wallet"
          FROM gro."DepositHandler_evt_LogNewDeposit"
         WHERE pwrd = true
        UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time)::date AS "week",
               "returnUsd" / 1e18 AS "amount",
               'withdrawal' AS "type",
               "user" AS "wallet"
          FROM gro."WithdrawHandler_evt_LogNewWithdrawal"
         WHERE pwrd = true
         UNION ALL
                 SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               "usdAmount" / 1e18 AS "amount",
               'deposit' AS "type",
               "user" AS "wallet"
          FROM gro."DepositHandlerPrev1_evt_LogNewDeposit"
         WHERE pwrd = true
        UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time)::date AS "week",
               "returnUsd" / 1e18 AS "amount",
               'withdrawal' AS "type",
               "user" AS "wallet"
          FROM gro."WithdrawHandlerPrev1_evt_LogNewWithdrawal"
         WHERE pwrd = true
         UNION ALL
                 SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               "usdAmount" / 1e18 AS "amount",
               'deposit' AS "type",
               "user" AS "wallet"
          FROM gro."DepositHandlerPrev2_evt_LogNewDeposit"
         WHERE pwrd = true
        UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time)::date AS "week",
               "returnUsd" / 1e18 AS "amount",
               'withdrawal' AS "type",
               "user" AS "wallet"
          FROM gro."WithdrawHandlerPrev2_evt_LogNewWithdrawal"
         WHERE pwrd = true
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
                         + SUM(COALESCE(deposit) - COALESCE(withdraw, 0)) OVER (PARTITION BY wallet ORDER BY week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) <= 0
                    THEN deposit
                    ELSE 0
                END AS "new_tvl",
                CASE
                    WHEN COALESCE(deposit, 0) > 0 
                     AND - COALESCE(deposit, 0) 
                         + COALESCE(withdraw, 0)
                         + SUM(COALESCE(deposit) - COALESCE(withdraw, 0)) OVER (PARTITION BY wallet ORDER BY week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) > 0
                    THEN deposit
                    ELSE 0
                END AS "renewed_tvl",
                COALESCE(withdraw, 0) as "churned_tvl"
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
                  +SUM(COALESCE(new_tvl, 0) + COALESCE(renewed_tvl, 0) - COALESCE(churned_tvl, 0)) OVER (ORDER BY period.week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) as "retained_tvl"
            FROM period
            LEFT JOIN kpis
                ON period.week = kpis.week
        ) ret
         GROUP BY 1
    )

SELECT p.week::date AS "week",
    kpis.new_tvl AS "New TVL",
    kpis.renewed_tvl AS "Renewed TVL",
    -kpis.churned_tvl AS "Churned TVL",
    rt.retained_tvl AS "Retained TVL",
    SUM(COALESCE(kpis.new_tvl, 0) + COALESCE(kpis.renewed_tvl) - COALESCE(kpis.churned_tvl,0)) OVER (ORDER BY p.week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) AS "Total TVL"
FROM period p
LEFT JOIN kpis
    ON p.week = kpis.week
LEFT JOIN retained_tvl rt
    ON p.week = rt.week
ORDER BY 1 DESC


-- version 1.0
/*
WITH
    -- Time period for the Dashboard
    period AS (
        --SELECT DISTINCT(date_trunc('week',time)) AS "week" FROM Ethereum.Blocks WHERE time >= '2021-05-24 00:00:00' ORDER BY 1 ASC
        SELECT generate_series('2021-05-24'::TIMESTAMP, date_trunc('week', NOW()), '1 week') AS "week"
    ),
    -- Deposits & withdrawals
    pwrd AS (
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               "usdAmount" / 1e18 AS "amount",
               'deposit' AS "type",
               "user" AS "wallet"
          FROM gro."DepositHandler_evt_LogNewDeposit"
         WHERE pwrd = true
        UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time)::date AS "week",
               "returnUsd" / 1e18 AS "amount",
               'withdrawal' AS "type",
               "user" AS "wallet"
          FROM gro."WithdrawHandler_evt_LogNewWithdrawal"
         WHERE pwrd = true
         UNION ALL
                 SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               "usdAmount" / 1e18 AS "amount",
               'deposit' AS "type",
               "user" AS "wallet"
          FROM gro."DepositHandlerPrev1_evt_LogNewDeposit"
         WHERE pwrd = true
        UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time)::date AS "week",
               "returnUsd" / 1e18 AS "amount",
               'withdrawal' AS "type",
               "user" AS "wallet"
          FROM gro."WithdrawHandlerPrev1_evt_LogNewWithdrawal"
         WHERE pwrd = true
         UNION ALL
                 SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               "usdAmount" / 1e18 AS "amount",
               'deposit' AS "type",
               "user" AS "wallet"
          FROM gro."DepositHandlerPrev2_evt_LogNewDeposit"
         WHERE pwrd = true
        UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time)::date AS "week",
               "returnUsd" / 1e18 AS "amount",
               'withdrawal' AS "type",
               "user" AS "wallet"
          FROM gro."WithdrawHandlerPrev2_evt_LogNewWithdrawal"
         WHERE pwrd = true
    ),
    -- Sum of deposits & withdrawals by week & wallet
    pwrd_acc AS (
        SELECT week, 
               wallet, 
               SUM(CASE WHEN type = 'deposit' THEN pwrd.amount END) as deposit,
               SUM(CASE WHEN type = 'withdrawal' THEN pwrd.amount END) as withdraw
          FROM pwrd
          --WHERE wallet in ('\x00ff6b7d26407a46af2b631b4fa452a036d027e5','\x1c76fe4c0aac7b9d29aab6d17fd0f075e7fc84a5') --to check sample data
        GROUP BY 1,2
    ),
    -- KPI calculations for New TVL, Renewed TVL & Churned TVL 
    kpis AS (
        SELECT period.week as "week",
               pwrd_acc.wallet as "wallet",
               pwrd_acc.deposit as "deposit_tvl",
               pwrd_acc.withdraw as "withdraw_tvl",
               COALESCE(sum(pwrd_acc_before.deposit), 0) AS "deposit_before", -- FOR INFO
               CASE WHEN pwrd_acc.deposit + COALESCE(sum(pwrd_acc_before.deposit), 0) = pwrd_acc.deposit
                    THEN pwrd_acc.deposit ELSE 0
               END as "new_tvl",
               CASE WHEN COALESCE(pwrd_acc.deposit, 0) > 0 
                    AND  COALESCE(sum(pwrd_acc_before.deposit), 0) > 0
                    THEN pwrd_acc.deposit ELSE 0
               END as "renewed_tvl",
               COALESCE(pwrd_acc.withdraw, 0) as "churned_tvl"
        FROM period
            LEFT JOIN pwrd_acc
            ON period.week = pwrd_acc.week
            LEFT JOIN pwrd_acc pwrd_acc_before
            ON pwrd_acc_before.week < period.week - interval '7 days'
            AND pwrd_acc.wallet = pwrd_acc_before.wallet
        GROUP BY 1,2,3,4
        ),
        -- KPI aggregations before calculating Retained TVL (can't use window with aggregate function)
        kpis_aggr AS (
            SELECT week,
                   sum(deposit_tvl) AS deposit_tvl,
                   sum(withdraw_tvl) AS withdraw_tvl,
                   sum(new_tvl) AS new_tvl,
                   sum(renewed_tvl) AS renewed_tvl,
                   sum(churned_tvl) AS churned_tvl
            FROM kpis
            GROUP BY 1
        ),
        -- KPI calculation for Retained TVL
        kpis_final AS (
            SELECT week::date AS "week",
                   new_tvl AS "New TVL",
                   renewed_tvl AS "Renewed TVL",
                   -churned_tvl AS "Churned TVL",
                   - COALESCE(deposit_tvl, 0)
                   --+ COALESCE(withdraw_tvl ,0)
                   --- COALESCE(churned_tvl, 0)
                   + COALESCE(sum(deposit_tvl) OVER (ORDER BY week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW), 0)
                   - COALESCE(sum(withdraw_tvl) OVER (ORDER BY week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW), 0) AS "Retained TVL",
                   COALESCE(sum(deposit_tvl) OVER (ORDER BY week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW), 0)
                   - COALESCE(sum(withdraw_tvl) OVER (ORDER BY week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW), 0) AS "Total TVL"
            FROM kpis_aggr
        )
        
SELECT * FROM kpis_final ORDER BY 1 ASC
*/