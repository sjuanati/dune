WITH 
    alloc_points AS (
        SELECT 
            "pid", 
            "allocPoint", 
            ROW_NUMBER() OVER (PARTITION BY "pid" ORDER BY "current_block" DESC) AS rn
        FROM (
            SELECT
                "evt_block_number" AS "current_block",
                "allocPoint" AS "allocPoint",
                "pid" AS "pid"
            FROM gro_ethereum.LPTokenStakerV2_evt_LogSetPool
            UNION ALL
            SELECT
                "evt_block_number" AS "current_block",
                "allocPoint" AS "allocPoint",
                "pid" AS "pid"
            FROM gro_ethereum.LPTokenStakerV2_evt_LogAddPool
        ) AS allocPoint
    ),
    alloc_points_latest AS (
        SELECT 
            CAST("pid" AS INTEGER) AS "pid", 
            CAST("allocPoint" AS DOUBLE) AS "allocPoint"
        FROM alloc_points
        WHERE rn = 1
    ),
    total_alloc_points AS (
        SELECT
            SUM("allocPoint") AS "total"
        FROM alloc_points_latest
    )

SELECT
    CASE
        WHEN "pid" = 0 THEN 'gro single sided'
        WHEN "pid" = 1 THEN 'vault gro uniswap'
        WHEN "pid" = 2 THEN 'gro usdc uniswap'
        WHEN "pid" = 3 THEN 'gvt single sided'
        WHEN "pid" = 4 THEN 'pwrd 3crv'
        WHEN "pid" = 5 THEN 'gro weth balancer'
        WHEN "pid" = 6 THEN 'pwrd single sided'
    END AS "pid",
    apl."allocPoint", 
    apl."allocPoint" / tap."total" AS "allocPoint %"
FROM alloc_points_latest apl
CROSS JOIN total_alloc_points tap
ORDER BY 2 DESC
