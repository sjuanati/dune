WITH
    period AS (
        SELECT generate_series('2022-03-24'::TIMESTAMP, date_trunc('day', NOW()), '1 day') AS "day"
    ),   
    gvt_in AS (
        SELECT date_trunc('day',evt_block_time) AS "day",
               SUM(value / 1e18)  AS "amount"
        FROM erc20."ERC20_evt_Transfer"
        WHERE "contract_address" = '\x3ADb04E127b9C0a5D36094125669d4603AC52a0c'  --gvt
        AND "to" = '\xc0ffee4a95f15ff9973a17e563a8a8701d719890' --c0ffee
        GROUP BY 1
    ),
    gvt_out AS (
        SELECT date_trunc('day',evt_block_time) AS "day",
               SUM(value / 1e18) AS "amount"
        FROM erc20."ERC20_evt_Transfer"
        WHERE "contract_address" = '\x3ADb04E127b9C0a5D36094125669d4603AC52a0c'  --gvt
        AND "from" = '\xc0ffee4a95f15ff9973a17e563a8a8701d719890' --c0ffee
        GROUP BY 1
    ),
    gvt_supply AS (
        SELECT
            evt_block_time AS "time",
            trades / 1e18 AS "trades"
        FROM (
            SELECT *,
                   CASE WHEN "from" = '\x0000000000000000000000000000000000000000' THEN value 
                        WHEN "to" = '\x0000000000000000000000000000000000000000' THEN -value
                   END as "trades"
            FROM erc20."ERC20_evt_Transfer" 
            where contract_address = '\x3adb04e127b9c0a5d36094125669d4603ac52a0c'
            ) a
    ),
    pnl_dates AS (
         SELECT date_trunc('day', "evt_block_time") AS "day", 
                max("evt_block_time") AS "max_time" 
         FROM gro."PnL_evt_LogPnLExecution"
         GROUP BY 1
    ),
    gvt_price AS (
        SELECT "day",
               max("gvt_tvl") / max("total_supply") AS "gvt_price" -- because PNL can be triggered twice for same timestamp
        FROM (
            SELECT pnl_dates.day AS "day",
                   (pnl."afterGvtAssets") / 1e18 as "gvt_tvl",
                   sum(trades) as "total_supply"
            FROM gro."PnL_evt_LogPnLExecution" pnl
            INNER JOIN pnl_dates pnl_dates
                 ON pnl.evt_block_time = pnl_dates.max_time
            LEFT JOIN gvt_supply
                ON gvt_supply."time" <= pnl_dates.max_time
            GROUP BY 1,2
        ) pnl
        GROUP BY 1
    )

SELECT period.day,
       coalesce(gvt_in.amount, 0) AS "GVT in",
       -coalesce(gvt_out.amount, 0) AS "GVT out",
       coalesce(gvt_price, 0) AS "GVT price",
       trunc(
            coalesce(sum(gvt_in.amount) OVER (ORDER BY period.day ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW), 0)
            - coalesce(sum(gvt_out.amount) OVER (ORDER BY period.day ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW), 0)
       ,2) AS "GVT Balance",
       trunc(
            (coalesce(sum(gvt_in.amount) OVER (ORDER BY period.day ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW), 0)
            - coalesce(sum(gvt_out.amount) OVER (ORDER BY period.day ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW), 0))
            * coalesce(gvt_price, 0)
       ,2) AS "USD Balance",
       coalesce(sum(gvt_in.amount) OVER (ORDER BY period.day ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW), 0)
       AS "All time GVT in",
       -coalesce(sum(gvt_out.amount) OVER (ORDER BY period.day ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW), 0)
       AS "All time GVT out"
FROM period
     LEFT JOIN gvt_in
        ON period.day = gvt_in.day
     LEFT JOIN gvt_out
        ON period.day = gvt_out.day
     LEFT JOIN gvt_price
        ON period.day = gvt_price.day
ORDER BY day DESC


