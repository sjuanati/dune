/*
/// @title  Wallet Acquisition, Retention & Churn for PWRD
/// @kpi    - New addr: Wallets that made more than $30 of deposits in the protocol during this period and had less than $30 in ALL prior periods
///         - Churned addr: Wallets that withdrew this period and have less than $30 left in the protocol
///         - Retained addr: Wallets that had more than $30 in the protocol both during this & previous period. Not necessarily deposited,
///           but could just be old deposits that are retained.
///         - Returning: wallets that previously had been in the protocol, left and returned [TBD]
/// @param  - USD Balance: this will change the $30 default value
/// @dev    - Wallet count based on deposit & withdrawal handler events
///         - Edge case 1: Having a filter of $30 PWRD, user A transfers $100 PWRD to User B. If User B withdraws $50 PWRD and had no previous deposits, 
///           it will be considered as churn even their balance remains >$30 PWRD
///         - Edge case 2: If the filter is set significantly high, for instance $100K, if a user makes two deposits of $70 and a withdrawal of $110 
///           during different weeks, the result will be <New addr>: 0 and <Churned addr>: -1, resulting in overall negative retained addresses
*/

-- version 2.0
WITH
    -- Time period for the Dashboard (must be a week date, i.e.: 2021-05-24, 2021-05-31...)
    period AS (
        SELECT generate_series('2021-05-24'::TIMESTAMP, date_trunc('week', NOW()), '1 week') AS "week"
    ),
    -- Deposits & withdrawals (incl. all three versions of handlers)
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
        SELECT period.week, 
               wallet, 
               COALESCE(SUM(CASE WHEN type = 'deposit' THEN pwrd.amount END), 0) as deposit,
               COALESCE(SUM(CASE WHEN type = 'withdrawal' THEN pwrd.amount END), 0) as withdraw
         FROM period
              LEFT JOIN pwrd
                ON period.week = pwrd.week
        GROUP BY 1,2
    ),
    -- Calculation of:
    -- a) New addresses (any address with a deposit above $30 and a balance below $30 in previous periods)
    -- b) Churned addresses (any address with a withdrawal this week esulting in a balance > $30 the previous week and a balance < $30 the current week)
    kpis AS (
        SELECT week,
               sum(new_addr) as new_addr,
               sum(churned_addr) as churned_addr
         FROM (
            SELECT pwrd_acc.week,
                pwrd_acc.wallet,
                pwrd_acc.deposit,
                pwrd_acc.withdraw,
                CASE
                    WHEN {{PWRD - USD balance}} = 0 THEN 0
                    WHEN COALESCE(pwrd_acc.deposit, 0) > {{PWRD - USD balance}} 
                     AND - pwrd_acc.deposit + pwrd_acc.withdraw + COALESCE(SUM(pwrd_acc.deposit - pwrd_acc.withdraw) OVER (PARTITION BY pwrd_acc.wallet ORDER BY pwrd_acc.week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW), 0) < {{PWRD - USD balance}}
                    THEN 1
                    ELSE 0
                END AS "new_addr",
                CASE
                    WHEN {{PWRD - USD balance}} = 0 THEN 0
                    WHEN COALESCE(pwrd_acc.withdraw, 0) > 0
                    AND COALESCE(SUM(pwrd_acc.deposit - pwrd_acc.withdraw) OVER (PARTITION BY pwrd_acc.wallet ORDER BY pwrd_acc.week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW), 0) <= {{PWRD - USD balance}}
                    AND - pwrd_acc.deposit + pwrd_acc.withdraw + COALESCE(SUM(pwrd_acc.deposit - pwrd_acc.withdraw) OVER (PARTITION BY pwrd_acc.wallet ORDER BY pwrd_acc.week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW), 0) > {{PWRD - USD balance}}
                    THEN 1 
                    ELSE 0
                END AS "churned_addr"
            FROM pwrd_acc
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
                  -new_addr + SUM(COALESCE(new_addr,0) - COALESCE(churned_addr,0)) OVER (ORDER BY period.week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) as "retained_addr"
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
                   LEFT JOIN pwrd_acc
                   ON pwrd_acc.week <= period.week
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
    -- Time period for the Dashboard (must be a week date, i.e.: 2021-05-24, 2021-05-31...)
    period AS (
        SELECT generate_series('2021-05-24'::TIMESTAMP, date_trunc('week', NOW()), '1 week') AS "week"
    ),
    -- Deposits & withdrawals (incl. all three versions of handlers)
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
    -- KPI calculations for New TVL, Renewed TVL & Churned TVL 
    kpis AS (
        SELECT period.week as "week",
               pwrd_acc.wallet as "wallet",
               pwrd_acc.deposit as "deposit_tvl",
               pwrd_acc.withdraw as "withdraw_tvl",
               CASE WHEN pwrd_acc.deposit > {{PWRD - USD balance}}
                    AND COALESCE(sum(pwrd_acc_before.deposit), 0) - COALESCE(sum(pwrd_acc_before.withdraw), 0) < {{PWRD - USD balance}}
                    THEN 1 ELSE 0
               END AS "new_addr",
               CASE WHEN COALESCE(pwrd_acc.withdraw, 0) > 0
                    AND COALESCE(sum(pwrd_acc_now.deposit), 0) - COALESCE(sum(pwrd_acc_now.withdraw), 0) < {{PWRD - USD balance}}
                    THEN 1 ELSE 0
               END AS "churned_addr"
         FROM period
              LEFT JOIN pwrd_acc
                ON period.week = pwrd_acc.week
              LEFT JOIN pwrd_acc pwrd_acc_before
                ON pwrd_acc_before.week < period.week - interval '7 days'
                AND pwrd_acc.wallet = pwrd_acc_before.wallet
              LEFT JOIN pwrd_acc pwrd_acc_now
                ON pwrd_acc_now.week <= period.week 
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
        -- Retained method 1: Wallets that had more than $30 in the protocol BOTH during the prior period and this period
        --retained AS (
        --    SELECT retained_current.week,
        --           COUNT(DISTINCT retained_current.wallet) as "retained_addr"
        --    FROM (
        --        SELECT period.week,
        --               wallet
        --          FROM period
        --               LEFT JOIN pwrd_acc
        --               ON pwrd_acc.week <= period.week
        --      GROUP BY 1,2
        --        HAVING sum(COALESCE(deposit, 0)) - sum(COALESCE(withdraw, 0)) > {{PWRD - USD balance}}
        --    ) retained_current,
        --    (
        --        SELECT period.week,
        --               wallet
        --          FROM period
        --               LEFT JOIN pwrd_acc
        --               ON pwrd_acc.week <= period.week - interval '7 days'
        --      GROUP BY 1,2
        --        HAVING sum(COALESCE(deposit, 0)) - sum(COALESCE(withdraw, 0)) > {{PWRD - USD balance}}
        --    ) retained_before
        --    WHERE retained_current.wallet = retained_before.wallet
        --      AND retained_current.week = retained_before.week
        --    GROUP BY 1
        --),
        -- Retained method 2: Wallets that had more than $30 in the protocol during this period
        --retained AS (
        --    SELECT week,
        --           COUNT(DISTINCT wallet) as "retained_addr"
        --    FROM (
        --        SELECT period.week,
        --               wallet
        --          FROM period
        --               LEFT JOIN pwrd_acc
        --               ON pwrd_acc.week <= period.week
        --      GROUP BY 1,2
        --        HAVING sum(COALESCE(deposit, 0)) - sum(COALESCE(withdraw, 0)) > {{PWRD - USD balance}}
        --    ) ret
        --    GROUP BY 1
        --),
        -- All unique wallets per week
        all_users AS (
            SELECT week,
                   COUNT(DISTINCT wallet) as "unique_addr"
            FROM (
                SELECT period.week,
                       wallet
                  FROM period
                       LEFT JOIN pwrd_acc
                       ON pwrd_acc.week <= period.week
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
                   --COALESCE(retained.retained_addr, 0) AS "Retained",
                   COALESCE(sum(new_addr) OVER (ORDER BY kpi.week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW), 0)
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
