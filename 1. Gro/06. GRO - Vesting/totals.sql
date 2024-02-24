/*
/// @title Vesting Overview
/// @db_engine: v2 Dune SQL
/// @purpose Provide aggregated vesting figures for rewards, team & investors
*/

WITH
    -- GROVesting
    vests AS (
        SELECT
            "user" AS "user",
            CAST("amount" AS DOUBLE) / 1e18 as "amount",
            CAST(json_extract_scalar(json_parse("vesting"), '$.startTime') AS INTEGER) as startTime
        FROM gro_ethereum.GROVesting_evt_LogVest
        UNION ALL
        SELECT
            "user" AS "user",
            CAST("amount" AS DOUBLE) / 1e18 as "amount",
            CAST(json_extract_scalar(json_parse("vesting"), '$.startTime') AS INTEGER) as startTime
        FROM gro_ethereum.GROVestingV2_evt_LogVest
        UNION ALL
        SELECT
            "user" AS "user",
            -(CAST("unlocked" AS DOUBLE) / 1e18 + CAST("penalty" AS DOUBLE) / 1e18) as "amount",
            0 as startTime
        FROM gro_ethereum.GROVesting_evt_LogExit
        UNION ALL
        SELECT
            "user" AS "user",
            -(CAST("unlocked" AS DOUBLE) / 1e18 + CAST("penalty" AS DOUBLE) / 1e18) as "amount",
            0 as startTime
        FROM gro_ethereum.GROVestingV2_evt_LogExit
        UNION ALL
        SELECT
            "user" AS "user",
            0 as "amount",
            CAST("newPeriod" AS INTEGER) as "startTime"
        FROM gro_ethereum.GROVesting_evt_LogExtend
        UNION ALL
        SELECT
            "user" AS "user",
            0 as "amount",
            CAST("newPeriod" AS INTEGER) as "startTime"
        FROM gro_ethereum.GROVestingV2_evt_LogExtend
    ),
    total_gro AS (
        SELECT
            "user" AS "user",
            sum("amount") AS "total_gro",
            max("startTime") AS "startTime"
        FROM vests
        GROUP BY 1
    ),
    -- All positions were fully vested in September'23 to allow GRO token redemption
    vesting_gro AS (
        SELECT
        "user" AS "user",
        "total_gro" AS "total_gro",
        CASE
            WHEN "startTime" + 31556952 > FLOOR(TO_UNIXTIME(current_timestamp))
                THEN "total_gro" - "total_gro" * (FLOOR(TO_UNIXTIME(current_timestamp)) - "startTime") / (31556952)
            ELSE 0
        END AS "vesting_gro",
        -- 0 AS "vesting_gro",
        CASE
            WHEN "startTime" + 31556952 > FLOOR(TO_UNIXTIME(current_timestamp))
                THEN "total_gro" * (FLOOR(TO_UNIXTIME(current_timestamp)) - "startTime") / (31556952)
            ELSE total_gro
        END AS "vested_gro"
        --"total_gro" AS "vested_gro"
        FROM total_gro
    ),
    rewards_vesting_totals AS (
        SELECT
            SUM("total_gro") AS "total",
            SUM("vesting_gro") AS "vesting",
            SUM("vested_gro") AS "vested"
        FROM vesting_gro
    ),
    -- GROTeamVesting
    team_start_date AS (
      SELECT * FROM query_3407758
    ),
    team_vests AS (
        SELECT 
            CAST(vest."id" AS INTEGER) as "id",
            vest."contributor" as "contributor",
            CASE
                WHEN stop_vest."contributor" IS NULL
                    THEN CAST(vest."amount" AS DOUBLE) / 1e18
                    ELSE CAST(stop_vest."unlocked" AS DOUBLE) / 1e18
                END
            AS "amount"
        FROM gro_ethereum.GROTeamVesting_evt_LogNewVest vest
        LEFT JOIN gro_ethereum.GROTeamVesting_evt_LogStoppedVesting stop_vest
            ON vest."contributor" = stop_vest."contributor"
            AND vest."id" = stop_vest."id"
    ),
    -- All team vesting positions were stopped on 4 October 2023, so 3-year vesting date is
    -- replaced by timestamp 1696454100 (04/10/2023)
    team_vesting as (
         SELECT tv."contributor" as "contributor",
                tv."id" as "id",
                current_timestamp as "current_ts",
                dates."start_date" as "start_date",
                tv."amount" as "total_gro",
                TO_UNIXTIME(current_timestamp) as "now",
                CASE
                    WHEN TO_UNIXTIME(current_timestamp) < dates."start_date" + 31556952 -- start date + cliff
                        THEN 0
                    WHEN TO_UNIXTIME(current_timestamp) < dates."start_date" + 94670856 -- start date + vesting time
                        --THEN tv."amount" * ( TO_UNIXTIME(current_timestamp) - dates."start_date" ) / 94670856
                        THEN tv."amount" * ( 1696454100 - dates."start_date" ) / 94670856
                    ELSE tv."amount"
                END as "vested_gro",
                CASE
                    WHEN TO_UNIXTIME(current_timestamp) < dates."start_date" + 31556952  -- start date + cliff
                        THEN tv.amount
                    WHEN TO_UNIXTIME(current_timestamp) - 31556952 > dates."start_date"
                        THEN tv."amount" - tv."amount" * ( TO_UNIXTIME(current_timestamp) - dates."start_date" ) / 94670856
                    ELSE 0
                END as "vesting_gro"
                -- 0 AS "vesting_gro"
        FROM team_vests tv
            LEFT JOIN team_start_date dates
            ON tv."contributor" = dates."contributor"
            AND tv."id" = dates."id"
    ),
    team_vesting_totals AS (
        SELECT
            SUM("total_gro") AS "total",
            SUM("vesting_gro") AS "vesting",
            SUM("vested_gro") AS "vested"
        FROM team_vesting
    ),
    -- total GRO unlocked based on => (QUOTA) * (block.timestamp - VESTING_START_TIME) / (VESTING_TIME)
    team_unlocked AS (
        SELECT (22509423 * ( (FLOOR(TO_UNIXTIME("time"))) - 1632844800) / 94670856) AS "amount"
        FROM ethereum.blocks ORDER BY "number" DESC LIMIT 1
    ),
    -- GRO directly withdrawn by contract owner (GRO vested not assigned to any wallet)
    team_withdrawn AS (
        SELECT SUM("amount") / 1e18 AS "amount"
        FROM gro_ethereum.GROTeamVesting_evt_LogWithdrawal
    ),
    -- GRO available (vested) not assigned to any wallet nor withdrawn yet
    team_available AS (
        SELECT u.amount - (v.total + w.amount) AS "amount"
        FROM team_vesting_totals v, team_withdrawn w, team_unlocked u
    ),
    -- GRO claimed (withdrawn by team member)
    team_claimed AS (
        SELECT SUM("amount") / 1e18 AS "amount"
        FROM gro_ethereum.GROTeamVesting_evt_LogClaimed
    ),
-- GROInvVesting
    investor_start_date AS (
        SELECT * FROM query_3407764
    ),
    investor_vests AS (
        SELECT vest."investor" as "investor",
               CAST(vest."amount" AS DOUBLE) / 1e18 as "amount"
        FROM gro_ethereum.GROInvVesting_evt_LogNewVest vest
    ),
    investor_vesting AS (
        SELECT iv."investor" as "investor",
               iv."amount" as "total_gro",
               CASE
                    WHEN TO_UNIXTIME(current_timestamp) < dates."start_date" + 31556952 -- start date + cliff
                        THEN 0
                    WHEN TO_UNIXTIME(current_timestamp) < dates."start_date" + 94670856 -- start date + vesting time
                        THEN iv."amount" * ( TO_UNIXTIME(current_timestamp) - dates."start_date" ) / 94670856
                    ELSE iv."amount"
               END as "vested_gro",
               CASE
                    WHEN TO_UNIXTIME(current_timestamp) < dates."start_date" + 31556952  -- start date + cliff
                        THEN iv."amount"
                    WHEN TO_UNIXTIME(current_timestamp) - 31556952 > dates."start_date"
                        THEN iv."amount" - iv."amount" * ( TO_UNIXTIME(current_timestamp) - dates."start_date" ) / 94670856
                    ELSE 0
               END as "vesting_gro"
    FROM investor_vests iv
        LEFT JOIN investor_start_date dates
        ON iv."investor" = dates."investor"
    ),
    investor_vesting_totals AS (
        SELECT
            SUM("total_gro") AS "total",
            SUM("vesting_gro") AS "vesting",
            SUM("vested_gro") AS "vested"
        FROM investor_vesting
    ),
    gro_price AS (
        SELECT (CAST(reserve1 AS DOUBLE) / 1e6) / (CAST(reserve0 AS DOUBLE) / 1e18) as "gro_price"
        FROM  uniswap_v2_ethereum.Pair_evt_Sync
        WHERE contract_address = 0x21c5918ccb42d20a2368bdca8feda0399ebfd2f6
        ORDER BY evt_block_number DESC
        LIMIT 1
    ),
    totals AS (
        SELECT
            r."vesting" + t."vesting" + i."vesting" AS "total_vesting_gro",
            (r."vesting" + t."vesting" + i."vesting") * price."gro_price" AS "total_vesting_usd",
            price."gro_price" AS "gro_price"
        FROM rewards_vesting_totals r
        CROSS JOIN team_vesting_totals t
        CROSS JOIN investor_vesting_totals i
        CROSS JOIN gro_price price
    )

SELECT 
    'Rewards' AS "type", 
    r."total" AS "total gro",
    r."vested" AS "vested gro",
    r."vesting" AS "vesting gro",
    r."vesting" * p."gro_price" AS "vesting usd",
    t."total_vesting_gro" AS "total_vesting_gro",
    t."total_vesting_usd" AS "total_vesting_usd",
    t."gro_price" AS "gro price"
FROM rewards_vesting_totals r
CROSS JOIN gro_price p
CROSS JOIN totals t
UNION ALL
SELECT
    'Investors' AS "type", 
    i."total" AS "total gro",
    i."vested" AS "vested gro",
    i."vesting" AS "vesting gro",
    i."vesting" * p."gro_price" AS "vesting usd",
    t."total_vesting_gro" AS "total_vesting_gro",
    t."total_vesting_usd" AS "total_vesting_usd",
    t."gro_price" AS "gro price"
FROM investor_vesting_totals i
CROSS JOIN gro_price p
CROSS JOIN totals t
UNION ALL
SELECT
    'Team' AS "type", 
    t."total" - c."amount" + a."amount" AS "total gro",
    t."vested" - c."amount" + a."amount" AS "vested gro",
    t."vesting" AS "vesting gro",
    t."vesting" * p."gro_price" AS "vesting usd",
    tt."total_vesting_gro" AS "total_vesting_gro",
    tt."total_vesting_usd" AS "total_vesting_usd",
    tt."gro_price" AS "gro price"
FROM team_vesting_totals t
CROSS JOIN gro_price p
CROSS JOIN totals tt
CROSS JOIN team_available a
CROSS JOIN team_claimed c
ORDER BY 2 DESC
