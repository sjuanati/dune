/*
/// @title Staker emissions
/// @db_engine: v2 Dune SQL
/// @purpose Provide the GRO rewards emitted through the Sushiswap's MasterChef (aka. LpTokenStaker) via the different Liquidity Pools
/// @contracts:
///     LpTokenStaker v1: 0x001C249c09090D79Dc350A286247479F08c7aaD7
///     LpTokenStaker v2: 0x2E32bAd45a1C29c1EA27cf4dD588DF9e68ED376C
*/

WITH
    last_block AS (
        SELECT MAX("number") AS "block" FROM ethereum.blocks
    ),
    gro_per_block AS (
        SELECT
            CAST("newGro" AS DOUBLE) / 1e18 AS "new_gro",
            "evt_block_time",
            "evt_block_number"
        FROM gro_ethereum.LPTokenStaker_evt_LogGroPerBlock
        UNION ALL
        SELECT
            CAST("newGro" AS DOUBLE) / 1e18 AS "new_gro",
            "evt_block_time",
            "evt_block_number"
        FROM gro_ethereum.LPTokenStakerV2_evt_LogGroPerBlock
    ),
    gro_emitted_ AS (
        SELECT
            "evt_block_time" AS "current_date",
            LEAD("evt_block_time", 1, current_date) OVER (ORDER BY "evt_block_number" ASC) AS "next_date",
            "evt_block_number" AS "current_block",
            LEAD("evt_block_number", 1, last_block."block") OVER (ORDER BY "evt_block_number" ASC) AS "next_block",
            "new_gro",
            (COALESCE(LEAD("evt_block_number", 1) OVER (ORDER BY "evt_block_number" ASC), last_block."block") - "evt_block_number") * "new_gro" AS "gro_emitted"
        FROM gro_per_block,
             last_block
    ),
    gro_emitted AS (
        SELECT
            "current_date",
            "next_date",
            "current_block",
            "next_block",
            "new_gro",
            "gro_emitted",
            SUM(COALESCE("gro_emitted", 0)) OVER (ORDER BY "current_block" ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) as "gro_total"
        FROM gro_emitted_
        ORDER BY current_block DESC
    ),
    allocPoint AS (
        SELECT
            "evt_block_number" AS "current_block",
            "evt_block_time" AS "current_date",
            "allocPoint" AS "allocPoint",
            "pid" AS "pid"
        FROM gro_ethereum.LPTokenStaker_evt_LogSetPool
        UNION ALL
        SELECT
            "evt_block_number" AS "current_block",
            "evt_block_time" AS "current_date",
            "allocPoint" AS "allocPoint",
            "pid" AS "pid"
        FROM gro_ethereum.LPTokenStakerV2_evt_LogSetPool
        UNION ALL
        SELECT
            "evt_block_number" AS "current_block",
            "evt_block_time" AS "current_date",
            "allocPoint" AS "allocPoint",
            "pid" AS "pid"
        FROM gro_ethereum.LPTokenStaker_evt_LogAddPool
        UNION ALL
        SELECT
            "evt_block_number" AS "current_block",
            "evt_block_time" AS "current_date",
            "allocPoint" AS "allocPoint",
            "pid" AS "pid"
        FROM gro_ethereum.LPTokenStakerV2_evt_LogAddPool
    ),
    alloc_point_total AS (
        SELECT
            "current_block",
            "current_date",
            LEAD("current_block", 1, lb."block") OVER (PARTITION BY "pid" ORDER BY "current_block" ASC) AS "next_block",
            LEAD("current_date", 1, current_timestamp) OVER (PARTITION BY "pid" ORDER BY "current_date" ASC) AS "next_date",
            "allocPoint",
            "pid"
        FROM allocPoint,
             last_block lb
        ORDER BY "current_block" DESC, "pid" DESC
    ),
    distinct_blocks AS (
        SELECT "current_block" AS "block", "current_date" AS "date"
        FROM gro_emitted
        UNION
        SELECT "next_block" AS "block", "next_date" AS "date"
        FROM gro_emitted
        UNION
        SELECT "current_block" AS "block", "current_date" AS "date"
        FROM alloc_point_total
        UNION
        SELECT "next_block" AS "block", "next_date" AS "date"
        FROM alloc_point_total
    ),
    intervals AS (
        SELECT 
            "block" AS "start_block",
            "date" AS "start_date",
            LEAD("block", 1, (SELECT MAX("number") FROM ethereum.blocks)) OVER (ORDER BY "block") AS "end_block",
            LEAD("date", 1, current_timestamp) OVER (ORDER BY "date") AS "end_date"
        FROM distinct_blocks
    ),
    interval_emissions AS (
        SELECT 
            i."start_block",
            i."end_block",
            i."start_date",
            i."end_date",
            g."new_gro"
        FROM intervals i
        LEFT JOIN gro_emitted g ON i."start_block" >= g."current_block" AND i."start_block" < g."next_block"
    ),
    interval_allocations AS (
        SELECT
            i."start_block",
            i."end_block",
            i."start_date",
            i."end_date",
            CAST(a."pid" AS INTEGER) AS "pid",
            CAST(a."allocPoint" AS DOUBLE) AS "allocPoint"
        FROM intervals i
        LEFT JOIN alloc_point_total a ON i."start_block" >= a."current_block" AND i."start_block" < a."next_block"
    ),
    total_allocations_sum AS (
        SELECT
            "start_block",
            "start_date",
            "end_block",
            "end_date",
            SUM("allocPoint") AS total_allocPoint
        FROM interval_allocations
        GROUP BY 1,2,3,4
    ),
    -- This is to set the total_allocPoint for the latest record. Otherwise, it would be empty
    total_allocations AS (
        SELECT
            "start_block",
            "end_block",
            "start_date",
            "end_date",
            CASE
                WHEN "total_allocPoint" IS NULL
                THEN COALESCE(LAG("total_allocPoint") IGNORE NULLS OVER (ORDER BY "start_block", "end_block"), "total_allocPoint")
                ELSE "total_allocPoint"
            END AS total_allocPoint
        FROM total_allocations_sum
    ),
    gro_emitted_per_pool AS (
        SELECT 
            a."start_block", 
            a."end_block", 
            a."start_date",
            a."end_date",
            a."pid",
            CASE
                WHEN a."pid" = 0 THEN 'gro single sided'
                WHEN a."pid" = 1 THEN 'vault gro uniswap'
                WHEN a."pid" = 2 THEN 'gro usdc uniswap'
                WHEN a."pid" = 3 THEN 'gvt single sided'
                WHEN a."pid" = 4 THEN 'pwrd 3crv'
                WHEN a."pid" = 5 THEN 'gro weth balancer'
                WHEN a."pid" = 6 THEN 'pwrd single sided'
            END AS pool_type,
            (a."end_block" - a."start_block") * e."new_gro" * (a."allocPoint" / t."total_allocPoint") AS "gro_amount"
        FROM interval_allocations a
        JOIN total_allocations t ON a."start_block" = t."start_block" AND a."end_block" = t."end_block"
        JOIN interval_emissions e ON a."start_block" = e."start_block" AND a."end_block" = e."end_block"
        WHERE a."pid" IS NOT NULL -- to remove the last interval where current and next block is the same (latest block)
        AND e."new_gro" IS NOT NULL -- to remove records at the very beginning where gro emission was not set
        AND t."total_allocPoint" > 0 -- to remove short periods when emission was stopped
    ),
    total_emitted AS (
        SELECT
            end_block,
            end_date,
            pool_type,
            SUM(COALESCE("gro_amount", 0)) OVER (PARTITION BY pool_type ORDER BY "end_block" ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) AS "gro_amount"
        FROM gro_emitted_per_pool
        ORDER BY 1 DESC
    )


SELECT * FROM total_emitted
