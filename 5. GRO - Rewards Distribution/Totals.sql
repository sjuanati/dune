WITH
    gro_per_block AS (
        SELECT
            CAST(newGro AS DOUBLE) / 1e18 AS "new_gro",
            evt_block_time,
            evt_block_number,
            'v1' as "version"
        FROM gro_ethereum.LPTokenStaker_evt_LogGroPerBlock
        UNION ALL
        SELECT
            CAST(newGro AS DOUBLE) / 1e18 AS "new_gro",
            evt_block_time,
            evt_block_number,
            'v2' as "version"
        FROM gro_ethereum.LPTokenStakerV2_evt_LogGroPerBlock
    ),
    last_block AS (SELECT number as "block" FROM ethereum.blocks ORDER BY time DESC LIMIT 1),
    gro_rewards AS (
        SELECT
            evt_block_time,
            evt_block_number,
            (COALESCE(LEAD(evt_block_number, 1) OVER (ORDER BY evt_block_number ASC), last_block.block) - evt_block_number) * new_gro as "gro_rewards",
            'staker' as "source",
            version
        FROM gro_per_block, last_block
        UNION ALL
        SELECT
            evt_block_time,
            evt_block_number,
            CAST(totalAmount AS DOUBLE) / 1e18 as "gro_rewards",
            'airdrop' as "source",
            'v1' as "version"
        FROM gro_ethereum.AirDrop_evt_LogNewDrop
        UNION ALL
        SELECT
            evt_block_time,
            evt_block_number,
            CAST(totalAmount AS DOUBLE) / 1e18 as "gro_rewards",
            'airdrop' as "source",
            'v2' as "version"
        FROM gro_ethereum.AirDropV2_evt_LogNewDrop
    )

SELECT
    evt_block_time as "date",
    gro_rewards,
    SUM(gro_rewards) OVER (ORDER BY evt_block_number ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) as "gro_rewards_total",
    source,
    version,
    evt_block_number as "block_number"
FROM gro_rewards
ORDER BY evt_block_number DESC
