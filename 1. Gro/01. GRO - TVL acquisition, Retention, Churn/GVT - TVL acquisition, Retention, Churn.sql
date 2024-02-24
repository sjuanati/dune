/*
/// @title  TVL Acquisition, Retention & Churn for GVT [Engine v2 Dune SQL]
/// @kpi    - New TVL: Deposits into the protocol during period by wallets that had never deposited into the protocol before
///         - Renewed TVL: Deposits into the protocol during period by wallets that had deposited into the protocol before
///         - Churned TVL: Withdrawals from the protocol during this period
///         - Retained: TVL in the protocol during the previous time period deducting churned TVL from this period
///         - Yield: TVL based on the diff between previous TVL and current retained TVL + churned TVL 
/// @dev    - PWRD: TVL based on deposit & withdrawal handler events, therefore excluding returns
///         - GVT: TVL based on transfer & PNL events, therefore including returns
*/

-- version 4.0 [Migrated to Dune SQL & included G2]

WITH
    -- Time period for the Dashboard (must be a week date, i.e.: 2021-05-24, 2021-05-31...)
    period AS (
        SELECT week
        FROM unnest(sequence(date('2021-05-24'), date_trunc('week', CURRENT_DATE), interval '7' day)) AS t(week)
    ),
    -- Total GVT supply based on ERC20 transfers
    gvt_total_supply AS (
        SELECT week,
            SUM(COALESCE(total_supply, 0)) OVER (ORDER BY week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) AS total_supply
        FROM (
                SELECT
                    date_trunc('week', evt_block_time) AS week, 
                    SUM(
                        CASE
                            WHEN "from" = 0x0000000000000000000000000000000000000000 THEN CAST(value AS DOUBLE)
                            WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -CAST(value AS DOUBLE)
                        END
                    ) / 1e18 AS total_supply
                FROM erc20_ethereum.evt_Transfer tr
                WHERE contract_address = 0x3ADb04E127b9C0a5D36094125669d4603AC52a0c
                GROUP BY 1
                ORDER BY 1 DESC
        ) gvt_supply
    ),
    -- GVT deposits & withdrawals based on ERC20 transfers
    gvt AS (
        SELECT date_trunc('week', evt_block_time) AS week, 
               CAST(value AS DOUBLE) / 1e18 AS amount,
               'deposit' AS type,
               to AS wallet
            FROM erc20_ethereum.evt_Transfer tr
            WHERE "contract_address" = 0x3ADb04E127b9C0a5D36094125669d4603AC52a0c
              AND "from" = 0x0000000000000000000000000000000000000000
        UNION ALL
        SELECT date_trunc('week', evt_block_time) AS week, 
               CAST(value AS DOUBLE) / 1e18 AS amount,
               'withdrawal' AS type,
               "from" AS wallet
            FROM erc20_ethereum.evt_Transfer tr
            WHERE "contract_address" = 0x3ADb04E127b9C0a5D36094125669d4603AC52a0c
              AND "to" = 0x0000000000000000000000000000000000000000
    ),
    -- Sum of deposits & withdrawals by week & wallet
    gvt_acc AS (
        SELECT week AS week, 
               wallet AS wallet, 
               SUM(CASE WHEN type = 'deposit' THEN gvt.amount END) AS deposit,
               SUM(CASE WHEN type = 'withdrawal' THEN gvt.amount END) AS withdraw
          FROM gvt
        GROUP BY 1,2
    ),
    -- Latest day of a week with a PnL event
    pnl_dates AS (
        SELECT date_trunc('week', evt_block_time) AS week, 
            max(evt_block_time) AS max_time 
        FROM gro_ethereum.PnL_evt_LogPnLExecution
        GROUP BY 1
    ),
    -- Latest day of a week with a New Tranche event
    gtranche_dates AS (
        SELECT date_trunc('week', evt_block_time) AS week, 
            max(evt_block_time) AS max_time 
        FROM gro_ethereum.GTranche_evt_LogNewTrancheBalance
        GROUP BY 1
    ),
    -- Utilisation Ratio & GVT totalAssets from PnL & GTranche
    pnl AS (
        SELECT week AS week,
               max(util) AS "Utilisation Ratio",
               max(gvt_assets) AS gvt_total_assets
        FROM (
            SELECT pnl_dates.week AS week,
                    CAST(pnl.afterGvtAssets AS DOUBLE) / 1e18 AS gvt_assets,
                   (CAST(pnl.afterPwrdAssets AS DOUBLE) / 1e18) /  (CAST(pnl.afterGvtAssets AS DOUBLE) / 1e18) AS util
            FROM gro_ethereum.PnL_evt_LogPnLExecution pnl
            INNER JOIN pnl_dates pnl_dates
                 ON pnl.evt_block_time = pnl_dates.max_time
        WHERE pnl_dates.week <= date('2023-02-27')
        ) pnl1
        GROUP BY 1
        UNION ALL
        SELECT week AS week,
               max(util) AS "Utilisation Ratio",
               max(gvt_assets) AS gvt_total_assets
        FROM (
            SELECT gtranche_dates.week AS week,
                   CAST(gtranche.balances[1] AS DOUBLE) / 1e18 AS gvt_assets,
                   CAST(_utilisation AS DOUBLE) / 10000 AS util
            FROM gro_ethereum.GTranche_evt_LogNewTrancheBalance gtranche
            INNER JOIN gtranche_dates gtranche_dates
                 ON gtranche.evt_block_time = gtranche_dates.max_time
            WHERE gtranche_dates.week > date('2023-02-27')
        ) pnl2
        GROUP BY 1
    -- Current GVT price based on totalAssets / totalSupply from PnL
    ), gvt_price AS (
        SELECT p.week AS week,
               pnl.gvt_total_assets / ts.total_supply AS gvt_price
          FROM period p
          LEFT JOIN pnl pnl
            ON p.week = pnl.week
          LEFT JOIN gvt_total_supply ts
            on p.week = ts.week
    -- GVT price on previous week based on totalAssets / totalSupply from PnL
    ), gvt_price_7d AS (
        SELECT current.week AS week,
               prev.gvt_price AS gvt_price_7d
          FROM gvt_price current,
               gvt_price prev
         WHERE current.week - interval '7' day = prev.week 
    ),
    -- Calculation of
    -- a) New TVL (deposits into the protocol during this period by wallets that had never deposited into the protocol before)
    -- b) Renewed TVL (deposits into the protocol during this period by wallets that had deposited into the protocol before)
    -- c) Churned TVL (withdrawals from the protocol during this period)
    kpis_a AS (
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
            FROM gvt_acc
           GROUP BY 1,2,3,4
         ) new
         GROUP BY 1
    ),
    -- Calculation of
    -- a) Retained TVL: TVL in the protocol during the previous time period deducting churned TVL from this period
    -- b) Previous TVL TVL on previous time period (to be used for the yield calculation)
    kpis_b AS (
        SELECT week,
           sum(retained_tvl) as retained_tvl,
           sum(prev_tvl) as prev_tvl
         FROM (
            SELECT period.week,
                  -COALESCE(new_tvl,0)
                  -COALESCE(renewed_tvl, 0)
                  +SUM(COALESCE(new_tvl, 0) + COALESCE(renewed_tvl, 0) - COALESCE(churned_tvl, 0)) OVER (ORDER BY period.week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) AS retained_tvl,
                  -COALESCE(new_tvl,0)
                  -COALESCE(renewed_tvl, 0)
                  +COALESCE(churned_tvl, 0)
                  +SUM(COALESCE(new_tvl, 0) + COALESCE(renewed_tvl, 0) - COALESCE(churned_tvl, 0)) OVER (ORDER BY period.week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) AS prev_tvl
            FROM period
            LEFT JOIN kpis_a
                ON period.week = kpis_a.week
            LEFT JOIN gvt_price
                ON period.week = gvt_price.week
        ) ret
         GROUP BY 1
    ),
    -- Calculation of Yield: diff between [retained TVL + churned TVL] on current period (at current GVT price) 
    --                       and [Total TVL] on previous period (at GVT price on previous period)
    yield AS (
        SELECT p.week,
               kpis_b.retained_tvl * gvt_price.gvt_price + kpis_a.churned_tvl * gvt_price.gvt_price - kpis_b.prev_tvl * gvt_price_7d.gvt_price_7d AS yield
          FROM period p
          LEFT JOIN kpis_b
            ON p.week = kpis_b.week
          LEFT JOIN gvt_price
            ON p.week = gvt_price.week
          LEFT JOIN gvt_price_7d
            ON p.week = gvt_price_7d.week
          LEFT JOIN kpis_a
            ON p.week = kpis_a.week
    )

SELECT 
    SUBSTR(CAST(p.week AS varchar), 1, 10) AS week,
    kpis_a.new_tvl * gvt_price AS "New TVL",
    kpis_a.renewed_tvl * gvt_price AS "Renewed TVL",
    -kpis_a.churned_tvl * gvt_price AS "Churned TVL",
    kpis_b.retained_tvl * gvt_price AS "Retained TVL",
    yield.yield AS "Yield",
    SUM(COALESCE(kpis_a.new_tvl, 0) + COALESCE(kpis_a.renewed_tvl, 0) - COALESCE(kpis_a.churned_tvl, 0)) OVER (ORDER BY p.week ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) * gvt_price AS "Total TVL",
    pnl."Utilisation Ratio" AS "Utilisation Ratio",
    gvt_price.gvt_price
FROM period p
LEFT JOIN kpis_a
    ON p.week = kpis_a.week
LEFT JOIN kpis_b
    ON p.week = kpis_b.week
LEFT JOIN yield
    ON p.week = yield.week
LEFT JOIN pnl pnl
    ON p.week = pnl.week
LEFT JOIN gvt_price gvt_price
    ON p.week = gvt_price.week
ORDER BY 1 DESC