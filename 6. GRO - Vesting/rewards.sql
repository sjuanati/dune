/*
/// @title Rewards vesting
/// @db_engine: v2 Dune SQL
/// @purpose Provide aggregated vesting figures for rewards
/// @dev Exclude users without vesting positions (only > 0.01 vesting gro)
/// @data_validation: can't be done gobally, but per user basis:
///     total = GROVesting(0x748218256AfE0A19a88EBEB2E0C5Ce86d2178360).totalBalance()
///     vesting = GROVesting(0x748218256AfE0A19a88EBEB2E0C5Ce86d2178360).vestingBalance()
///     vested = GROVesting(0x748218256AfE0A19a88EBEB2E0C5Ce86d2178360).vestedBalance()
*/

WITH
    rewards_total_gro AS (
        SELECT
            "user" AS "user",
            CAST("amount" AS DOUBLE) / 1e18 as "amount",
            CAST(json_extract_scalar(json_parse("vesting"), '$.startTime') AS INTEGER) as "startTime"
        FROM gro_ethereum.GROVesting_evt_LogVest
        UNION ALL
        SELECT
            "user" AS "user",
            CAST("amount" AS DOUBLE) / 1e18 as "amount",
            CAST(json_extract_scalar(json_parse("vesting"), '$.startTime') AS INTEGER) as "startTime"
        FROM gro_ethereum.GROVestingV2_evt_LogVest vest
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
            CAST(newPeriod AS INTEGER) as startTime
        FROM gro_ethereum.GROVesting_evt_LogExtend
        UNION ALL
        SELECT
            "user" AS "user",
            0 as "amount",
            CAST(newPeriod AS INTEGER) as startTime
        FROM gro_ethereum.GROVestingV2_evt_LogExtend
    ),
    rewards_total_gro_acc AS (
        SELECT "user" AS "user", sum("amount") AS "total_gro", max("startTime") AS "startTime" FROM rewards_total_gro GROUP BY 1
    ),
    rewards_vesting AS (
        SELECT
            "user" AS "user",
            "startTime" AS "startDate",
            "total_gro" AS "total_gro",
            CASE
                WHEN "startTime" + 31556952 > FLOOR(TO_UNIXTIME(current_timestamp))
                    THEN "total_gro" - "total_gro" * (FLOOR(TO_UNIXTIME(current_timestamp)) - "startTime") / (31556952)
                ELSE 0
            END as "vesting_gro",
            CASE
                WHEN "startTime" + 31556952 > FLOOR(TO_UNIXTIME(current_timestamp))
                    THEN "total_gro" * (FLOOR(TO_UNIXTIME(current_timestamp)) - "startTime") / (31556952)
                ELSE total_gro
            END as "vested_gro"
        FROM rewards_total_gro_acc
    )
    
SELECT
    "user",
    "startDate" AS "startTS",
    from_unixtime("startDate") AS "startDate",
    "total_gro" AS "total",
    "vesting_gro" AS "vesting",
    "vested_gro" AS "vested"
FROM rewards_vesting
WHERE "vesting_gro" > 0.01
ORDER BY "total_gro" DESC
