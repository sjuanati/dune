WITH
    new_vest AS (
        SELECT
            "contributor" AS "contributor",
            SUM(CAST("amount" AS DOUBLE) / 1e18) AS "allocated",
            COUNT("contributor") AS "positions"
        FROM gro_ethereum.GROTeamVesting_evt_LogNewVest
        GROUP BY 1
    ),
    claims AS (
        SELECT
             "contributor" AS "contributor",
             SUM(CAST("amount" AS DOUBLE) / 1e18) AS "claimed"
        FROM gro_ethereum.GROTeamVesting_evt_LogClaimed
        GROUP BY 1
    ),
    stops AS (
        SELECT
            "contributor" AS "contributor",
            SUM(CAST("unlocked" AS DOUBLE) / 1e18) AS "unlocked_amount",
            COUNT("id") AS "locked"
        FROM gro_ethereum.GROTeamVesting_evt_LogStoppedVesting
        GROUP BY 1
    ),
    active AS (
        SELECT
            vest."contributor" AS "contributor",
            SUM(
                CASE
                    WHEN CAST(stop."id" AS INTEGER) >= 0 THEN 0
                    ELSE CAST(vest."amount" AS DOUBLE) / 1e18
                END
            ) AS "active"
        FROM gro_ethereum.GROTeamVesting_evt_LogNewVest vest
            LEFT JOIN gro_ethereum.GROTeamVesting_evt_LogStoppedVesting stop
                ON stop."contributor" = vest."contributor"
                AND stop."id" = vest."id"
        GROUP BY 1
    ),
    contributors AS (
        SELECT COUNT(DISTINCT(contributor)) AS "wallets"
        FROM gro_ethereum.GROTeamVesting_evt_LogNewVest
    )

SELECT nv.contributor AS "contributor",
       nv.allocated AS "allocated",
       a.active AS "active",
       COALESCE(c.claimed, 0) AS "claimed",
       COALESCE(nv.positions, 0) AS "positions",
       COALESCE(s.locked, 0) AS "locked",
       contrib."wallets" AS "wallets",
       SUM(nv.allocated) OVER (ORDER BY nv.allocated ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) AS "allocated_acc",
       SUM(a.active) OVER (ORDER BY nv.allocated ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) AS "active_acc",
       SUM(COALESCE(c.claimed,0)) OVER (ORDER BY nv.allocated ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) AS "claimed_acc"
FROM new_vest nv
    LEFT JOIN contributors contrib ON 1=1
    LEFT JOIN active a ON nv.contributor = a.contributor
    LEFT JOIN claims c ON nv.contributor = c.contributor
    LEFT JOIN stops s ON nv.contributor = s.contributor
GROUP BY 1,2,3,4,5,6,7
ORDER BY 2 DESC
