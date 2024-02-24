/*
/// @title    Wallet Acquisition, Retention & Churn for GVT
/// @Version  4.0 [Migrated to Engine v2 Dune SQL & included G2]
/// @kpi    - New addr: Wallets that made more than $30 of deposits in the protocol during this period and had less than $30 in ALL prior periods
///         - Churned addr: Wallets that withdrew this period and have less than $30 left in the protocol
///         - Retained addr: Wallets that had more than $30 in the protocol both during current & previous periods. Not necessarily deposited,
///           but could just be old deposits that are retained
/// @param  - USD Balance: this will change the $30 default value
/// @dev    - Wallet count based on deposit & withdrawal handler events & GRouter events, therefore EXCLUDING DIRECT TRANSFERS BETWEEN USERS
///         - Edge case 1: Having a filter of $30 GVT, user A transfers $100 GVT to User B. If User B withdraws $50 GVT and had no previous deposits, 
///           it will be considered as churn even their balance remains >$30 GVT
///         - Edge case 2: If the filter is set significantly high, for instance $100K GVT, if a user makes two deposits of $70 GVT and a withdrawal of $110 GVT
///           during different weeks, the result will be <New addr>: 0 and <Churned addr>: -1, resulting in overall negative retained addresses
*/

WITH
    -- Time period for the Dashboard
    period AS (
        SELECT week
        FROM unnest(sequence(date('2021-05-24'), date_trunc('week', CURRENT_DATE), interval '7' day)) AS t(week)
    ),
    -- Deposits & withdrawals (incl. all three versions of handlers)
    gvt AS (
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               CAST(usdAmount AS DOUBLE) / 1e18 AS "amount",
               'deposit' AS "type",
               user AS "wallet"
          FROM gro_ethereum.DepositHandler_evt_LogNewDeposit
         WHERE pwrd = false
        UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               CAST(returnUsd AS DOUBLE) / 1e18 AS "amount",
               'withdrawal' AS "type",
               user AS "wallet"
          FROM gro_ethereum.WithdrawHandler_evt_LogNewWithdrawal
         WHERE pwrd = false
         UNION ALL
                 SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               CAST(usdAmount AS DOUBLE) / 1e18 AS "amount",
               'deposit' AS "type",
               user AS "wallet"
          FROM gro_ethereum.DepositHandlerPrev1_evt_LogNewDeposit
         WHERE pwrd = false
        UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               CAST(returnUsd AS DOUBLE) / 1e18 AS "amount",
               'withdrawal' AS "type",
               user AS "wallet"
          FROM gro_ethereum.WithdrawHandlerPrev1_evt_LogNewWithdrawal
         WHERE pwrd = false
         UNION ALL
                 SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               CAST(usdAmount AS DOUBLE) / 1e18 AS "amount",
               'deposit' AS "type",
               user AS "wallet"
          FROM gro_ethereum.DepositHandlerPrev2_evt_LogNewDeposit
         WHERE pwrd = false
        UNION ALL
        SELECT evt_block_time AS "time",
               date_trunc('week', evt_block_time) AS "week",
               CAST(returnUsd AS DOUBLE) / 1e18 AS "amount",
               'withdrawal' AS "type",
               user AS "wallet"
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
               CAST(calcAmount AS DOUBLE) / 1e6 AS "amount",
               'withdrawal' AS "type",
               "sender" AS "wallet"
          FROM gro_ethereum.GRouter_evt_LogWithdrawal
         WHERE tranche = false
    ),
    -- Sum of deposits & withdrawals by week & wallet
    gvt_acc AS (
        SELECT period.week, 
               wallet,
               SUM(CASE WHEN type = 'deposit' THEN gvt.amount END) as "deposit",
               SUM(CASE WHEN type = 'withdrawal' THEN gvt.amount END) as "withdraw"
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
               sum(new_addr) as "new_addr",
               sum(churned_addr) as "churned_addr"
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
                         + SUM(COALESCE(gvt_acc.deposit, 0) - COALESCE(gvt_acc.withdraw, 0)) OVER (PARTITION BY gvt_acc.wallet ORDER BY gvt_acc.week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) < {{GVT - USD balance}}
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
           sum(retained_addr) as "retained_addr"
         FROM (
            SELECT period.week,
                  -new_addr + SUM(COALESCE(new_addr, 0) - COALESCE(churned_addr, 0)) OVER (ORDER BY period.week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) as retained_addr
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
        SELECT 
            SUBSTR(CAST(p.week AS varchar), 1, 10) AS "week",
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
