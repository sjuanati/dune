WITH
    gro AS (
        SELECT
            date_trunc('week', "evt_block_time") AS "date",
            CASE
                WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -CAST("value" AS DOUBLE)
                WHEN "from" = 0x0000000000000000000000000000000000000000 THEN CAST("value" AS DOUBLE)
            END / 1e18 AS "amount"
        FROM gro_ethereum.GROToken_evt_Transfer
    ),
    weekly_gro AS (
        SELECT
            "date",
            SUM("amount") AS "amount"
        FROM gro
        GROUP BY "date"
        ORDER BY "date" ASC
    ),
    gro_cumulative AS (
        SELECT
            "date",
            SUM(COALESCE(amount, 0)) OVER (ORDER BY "date" ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) as "amount"
        FROM weekly_gro
    )
   
SELECT * FROM gro_cumulative
