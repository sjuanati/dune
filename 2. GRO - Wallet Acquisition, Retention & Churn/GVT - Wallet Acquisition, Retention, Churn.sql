/*
/// @title  Wallet Acquisition, Retention & Churn for GVT
/// @kpi    - New addr: Wallets that made more than $30 of deposits in the protocol during this period and had less than $30 in ALL prior periods
///         - Churned addr: Wallets that withdrew this period and have less than $30 left in the protocol
///         - Retained addr: Wallets that had more than $30 in the protocol both during this period. Not necessarily deposited,
///           but could just be old deposits that are retained
/// @param  - USD Balance: this will change the $30 default value
/// @dev    - Wallet count based on deposit & withdrawal handler events
///         - Edge case 1: Having a filter of $30 GVT, user A transfers $100 GVT to User B. If User B withdraws $50 GVT and had no previous deposits, 
///           it will be considered as churn even their balance remains >$30 GVT
///         - Edge case 2: If the filter is set significantly high, for instance $100K GVT, if a user makes two deposits of $70 GVT and a withdrawal of $110 GVT
///           during different weeks, the result will be <New addr>: 0 and <Churned addr>: -1, resulting in overall negative retained addresses
*/

-- version 2.0
WITH
    -- Time period for the Dashboard
    period AS (
        --SELECT DISTINCT(date_trunc('week', time)) AS "week" FROM Ethereum.Blocks WHERE time >= '2021-05-24 00:00:00' ORDER BY 1 ASC
        SELECT generate_series('2021-05-24'::TIMESTAMP, date_trunc('week', NOW()), '1 week') AS "week"
    ),
    -- Deposits & withdrawals (incl. all three versions of handlers)
    gvt AS (
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               "usdAmount" / 1e18 AS "amount",
               'deposit' AS "type",
               "user" AS "wallet"
          FROM gro."DepositHandler_evt_LogNewDeposit"
         WHERE pwrd = false
        UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time)::date AS "week",
               "returnUsd" / 1e18 AS "amount",
               'withdrawal' AS "type",
               "user" AS "wallet"
          FROM gro."WithdrawHandler_evt_LogNewWithdrawal"
         WHERE pwrd = false
         UNION ALL
                 SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               "usdAmount" / 1e18 AS "amount",
               'deposit' AS "type",
               "user" AS "wallet"
          FROM gro."DepositHandlerPrev1_evt_LogNewDeposit"
         WHERE pwrd = false
        UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time)::date AS "week",
               "returnUsd" / 1e18 AS "amount",
               'withdrawal' AS "type",
               "user" AS "wallet"
          FROM gro."WithdrawHandlerPrev1_evt_LogNewWithdrawal"
         WHERE pwrd = false
         UNION ALL
                 SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               "usdAmount" / 1e18 AS "amount",
               'deposit' AS "type",
               "user" AS "wallet"
          FROM gro."DepositHandlerPrev2_evt_LogNewDeposit"
         WHERE pwrd = false
        UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time)::date AS "week",
               "returnUsd" / 1e18 AS "amount",
               'withdrawal' AS "type",
               "user" AS "wallet"
          FROM gro."WithdrawHandlerPrev2_evt_LogNewWithdrawal"
         WHERE pwrd = false
    ),
    -- Sum of deposits & withdrawals by week & wallet
    gvt_acc AS (
        SELECT period.week, 
               wallet,
               SUM(CASE WHEN type = 'deposit' THEN gvt.amount END) as deposit,
               SUM(CASE WHEN type = 'withdrawal' THEN gvt.amount END) as withdraw
         FROM period
              LEFT JOIN gvt
                ON period.week = gvt.week
        GROUP BY 1,2
    ),
    -- Calculation of:
    -- a) New addresses (any address with a deposit above $30 and a balance below $30 in previous periods)
    -- b) Churned addresses (any address with a withdrawal this week resulting in a balance > $30 the previous week and a balance < $30 the current week)
    kpis AS (
        SELECT week,
               sum(new_addr) as new_addr,
               sum(churned_addr) as churned_addr
         FROM (
            SELECT gvt_acc.week,
                gvt_acc.wallet,
                gvt_acc.deposit,
                gvt_acc.withdraw,
                CASE
                    WHEN {{GVT - USD balance}} = 0 THEN 0
                    WHEN COALESCE(gvt_acc.deposit, 0) > {{GVT - USD balance}} 
                     AND - COALESCE(gvt_acc.deposit, 0) 
                         + COALESCE(gvt_acc.withdraw, 0)
                         + SUM(COALESCE(gvt_acc.deposit) - COALESCE(gvt_acc.withdraw, 0)) OVER (PARTITION BY gvt_acc.wallet ORDER BY gvt_acc.week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) < {{GVT - USD balance}}
                    THEN 1
                    ELSE 0
                END AS "new_addr",
                CASE
                    WHEN {{GVT - USD balance}} = 0 THEN 0
                    WHEN COALESCE(gvt_acc.withdraw, 0) > 0
                     AND SUM(COALESCE(gvt_acc.deposit, 0) - COALESCE(gvt_acc.withdraw, 0)) OVER (PARTITION BY gvt_acc.wallet ORDER BY gvt_acc.week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) <= {{GVT - USD balance}}
                     AND - COALESCE(gvt_acc.deposit, 0) 
                         + COALESCE(gvt_acc.withdraw, 0)
                         + SUM(COALESCE(gvt_acc.deposit, 0) - COALESCE(gvt_acc.withdraw, 0)) OVER (PARTITION BY gvt_acc.wallet ORDER BY gvt_acc.week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) > {{GVT - USD balance}}
                    THEN 1 
                    ELSE 0
                END AS "churned_addr"
            FROM gvt_acc
           GROUP BY 1,2,3,4
         ) new
         GROUP BY 1
    ),
    -- Calculation of retained addresses (any address that had a balance of $30 this and the previous week)
    retained_addr AS (
        SELECT week,
           sum(retained_addr) as retained_addr
         FROM (
            SELECT period.week,
                  -new_addr + SUM(COALESCE(new_addr, 0) - COALESCE(churned_addr, 0)) OVER (ORDER BY period.week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) as "retained_addr"
            FROM period
            LEFT JOIN kpis
                ON period.week = kpis.week
        ) ret
         GROUP BY 1
    ),
    -- All unique wallets per week
    all_users AS (
        SELECT week,
               COUNT(DISTINCT wallet) as "unique_addr"
        FROM (
            SELECT period.week,
                   wallet
              FROM period
                   LEFT JOIN gvt_acc
                   ON gvt_acc.week <= period.week
          GROUP BY 1,2
        ) ret
        GROUP BY 1
    ),
    -- Final KPI calculation
        kpis_final AS (
        SELECT p.week::date AS "week",
            kpis.new_addr AS "New",
            -kpis.churned_addr AS "Churned",
            ra.retained_addr AS "Retained",
            all_users.unique_addr AS "All Time"
        FROM period p
        LEFT JOIN kpis kpis
            ON p.week = kpis.week
        LEFT JOIN retained_addr ra
            ON p.week = ra.week
        LEFT JOIN all_users
            ON p.week = all_users.week
    )

SELECT * FROM kpis_final ORDER BY 1 DESC

-- version 1.0
/*
WITH
    -- Time period for the Dashboard
    period AS (
        --SELECT DISTINCT(date_trunc('week', time)) AS "week" FROM Ethereum.Blocks WHERE time >= '2021-05-24 00:00:00' ORDER BY 1 ASC
        SELECT generate_series('2021-05-24'::TIMESTAMP, date_trunc('week', NOW()), '1 week') AS "week"
    ),
    -- Deposits & withdrawals (incl. all three versions of handlers)
    gvt AS (
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               "usdAmount" / 1e18 AS "amount",
               'deposit' AS "type",
               "user" AS "wallet"
          FROM gro."DepositHandler_evt_LogNewDeposit"
         WHERE pwrd = false
        UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time)::date AS "week",
               "returnUsd" / 1e18 AS "amount",
               'withdrawal' AS "type",
               "user" AS "wallet"
          FROM gro."WithdrawHandler_evt_LogNewWithdrawal"
         WHERE pwrd = false
         UNION ALL
                 SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               "usdAmount" / 1e18 AS "amount",
               'deposit' AS "type",
               "user" AS "wallet"
          FROM gro."DepositHandlerPrev1_evt_LogNewDeposit"
         WHERE pwrd = false
        UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time)::date AS "week",
               "returnUsd" / 1e18 AS "amount",
               'withdrawal' AS "type",
               "user" AS "wallet"
          FROM gro."WithdrawHandlerPrev1_evt_LogNewWithdrawal"
         WHERE pwrd = false
         UNION ALL
                 SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               "usdAmount" / 1e18 AS "amount",
               'deposit' AS "type",
               "user" AS "wallet"
          FROM gro."DepositHandlerPrev2_evt_LogNewDeposit"
         WHERE pwrd = false
        UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time)::date AS "week",
               "returnUsd" / 1e18 AS "amount",
               'withdrawal' AS "type",
               "user" AS "wallet"
          FROM gro."WithdrawHandlerPrev2_evt_LogNewWithdrawal"
         WHERE pwrd = false
    ),
    -- Sum of deposits & withdrawals by week & wallet
    gvt_acc AS (
        SELECT week, 
               wallet, 
               SUM(CASE WHEN type = 'deposit' THEN gvt.amount END) as deposit,
               SUM(CASE WHEN type = 'withdrawal' THEN gvt.amount END) as withdraw
          FROM gvt
        GROUP BY 1,2
    ),
    -- KPI calculations for New TVL, Renewed TVL & Churned TVL 
    kpis AS (
        SELECT period.week as "week",
               gvt_acc.wallet as "wallet",
               gvt_acc.deposit as "deposit_tvl",
               gvt_acc.withdraw as "withdraw_tvl",
               CASE WHEN gvt_acc.deposit > {{GVT - USD balance}}
                    AND COALESCE(sum(gvt_acc_before.deposit), 0) - COALESCE(sum(gvt_acc_before.withdraw), 0) < {{GVT - USD balance}}
                    THEN 1 ELSE 0
               END AS "new_addr",
               CASE WHEN COALESCE(gvt_acc.withdraw, 0) > 0
                    AND COALESCE(sum(gvt_acc.deposit), 0) - COALESCE(sum(gvt_acc.withdraw), 0) < {{GVT - USD balance}}
                    THEN 1 ELSE 0
               END AS "churned_addr"
         FROM period
              LEFT JOIN gvt_acc
                ON period.week = gvt_acc.week
              LEFT JOIN gvt_acc gvt_acc_before
                ON gvt_acc_before.week < period.week - interval '7 days'
                AND gvt_acc.wallet = gvt_acc_before.wallet
              LEFT JOIN gvt_acc gvt_acc_now
                ON gvt_acc_now.week <= period.week 
        GROUP BY 1,2,3,4
        ),
        -- KPI aggregations before calculating Retained TVL (can't use window with aggregate function)
        kpis_aggr AS (
            SELECT week,
                   sum(deposit_tvl) AS deposit_tvl,
                   sum(withdraw_tvl) AS withdraw_tvl,
                   sum(new_addr) AS new_addr,
                   sum(churned_addr) AS churned_addr
            FROM kpis
            GROUP BY 1
        ),
        -- All unique wallets per week
        all_users AS (
            SELECT week,
                   COUNT(DISTINCT wallet) as "unique_addr"
            FROM (
                SELECT period.week,
                       wallet
                  FROM period
                       LEFT JOIN gvt_acc
                       ON gvt_acc.week <= period.week
              GROUP BY 1,2
            ) ret
            GROUP BY 1
        ),
        -- KPI calculation for Retained TVL
        kpis_final AS (
            SELECT kpi.week::date AS "week",
                   new_addr AS "New",
                   -churned_addr AS "Churned",
                   - COALESCE(new_addr, 0)
                   + COALESCE(sum(new_addr) OVER (ORDER BY kpi.week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW), 0)
                   - COALESCE(sum(churned_addr) OVER (ORDER BY kpi.week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW), 0)
                   AS "Retained",
                   + COALESCE(sum(new_addr) OVER (ORDER BY kpi.week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW), 0)
                   - COALESCE(sum(churned_addr) OVER (ORDER BY kpi.week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW), 0)
                   AS "Total",
                   all_users.unique_addr AS "All Time"
            FROM kpis_aggr kpi,
                 --LEFT JOIN retained
                 --   ON kpi.week = retained.week,
                 all_users
            WHERE kpi.week = all_users.week
        )

    SELECT * FROM kpis_final ORDER BY 1 ASC
*/