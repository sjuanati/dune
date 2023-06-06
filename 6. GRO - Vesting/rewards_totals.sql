WITH gro_vesting AS (
    SELECT 
        evt_block_time,
        CAST("totalLockedAmount" AS DOUBLE) / 1e18 as vesting_gro,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('day', evt_block_time) ORDER BY evt_block_time DESC) as rn
    FROM gro_ethereum.GROVesting_evt_LogVest
    UNION ALL
    SELECT 
        evt_block_time,
        CAST("totalLockedAmount" AS DOUBLE) / 1e18 as vesting_gro,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('day', evt_block_time) ORDER BY evt_block_time DESC) as rn
    FROM gro_ethereum.GROVesting_evt_LogExit
    UNION ALL
    SELECT 
        evt_block_time,
        CAST("totalLockedAmount" AS DOUBLE) / 1e18 as vesting_gro,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('day', evt_block_time) ORDER BY evt_block_time DESC) as rn
    FROM gro_ethereum.GROVestingV2_evt_LogVest
    UNION ALL
    SELECT 
        evt_block_time,
        CAST("totalLockedAmount" AS DOUBLE) / 1e18 as vesting_gro,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('day', evt_block_time) ORDER BY evt_block_time DESC) as rn
    FROM gro_ethereum.GROVestingV2_evt_LogExit
)

-- get the max value per day
SELECT 
    SUBSTR(CAST(DATE_TRUNC('day', evt_block_time) AS varchar), 1, 10) as "date",
    vesting_gro AS "total_gro"
FROM gro_vesting
WHERE rn = 1
ORDER BY 1 DESC
