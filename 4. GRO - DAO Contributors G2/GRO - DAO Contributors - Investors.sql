WITH
    new_vest AS (
        SELECT
            "investor" AS "investor",
            SUM(CAST("amount" AS DOUBLE) / 1e18) AS "allocated"
        FROM gro_ethereum.GROInvVesting_evt_LogNewVest
        GROUP BY 1
    ),
    claims AS (
        SELECT
             "investor" AS "investor",
             SUM(CAST("amount" AS DOUBLE) / 1e18) AS "claimed"
        FROM gro_ethereum.GROInvVesting_evt_LogClaimed
        GROUP BY 1
    ),
    contributors AS (
        SELECT COUNT(DISTINCT(investor)) AS "wallets"
        FROM gro_ethereum.GROInvVesting_evt_LogNewVest
    )

SELECT nv.investor AS "investor",
       nv.allocated AS "allocated",
       nv.allocated AS "active",
       COALESCE(c.claimed, 0) AS "claimed",
       contrib."wallets" AS "wallets",
       SUM(nv.allocated) OVER (ORDER BY nv.allocated ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) AS "allocated_acc",
       SUM(COALESCE(c.claimed,0)) OVER (ORDER BY nv.allocated ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) AS "claimed_acc"
FROM new_vest nv
    LEFT JOIN contributors contrib ON 1=1
    LEFT JOIN claims c ON nv.investor = c.investor
GROUP BY 1,2,3,4,5
ORDER BY 2 DESC
