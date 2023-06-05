-- GRO emitted from LpTokenStaker V1 & V2
WITH
    gro_per_block AS (
        SELECT
            CAST(newGro AS DOUBLE) / 1e18 AS "new_gro",
            contract_address,
            'v1' as "version",
            evt_tx_hash,
            evt_block_time,
            evt_block_number
        FROM gro_ethereum.LPTokenStaker_evt_LogGroPerBlock
        UNION ALL
        SELECT
            CAST(newGro AS DOUBLE) / 1e18 AS "new_gro",
            contract_address,
            'v2' as "version",
            evt_tx_hash,
            evt_block_time,
            evt_block_number
        FROM gro_ethereum.LPTokenStakerV2_evt_LogGroPerBlock
    ),
    last_block AS (
        SELECT number as "block" FROM ethereum.blocks ORDER BY time DESC LIMIT 1
    ),
    gro_emitted AS (
        SELECT
            evt_block_time,
            new_gro,
            evt_block_number,
            (COALESCE(LEAD(evt_block_number, 1) OVER (ORDER BY evt_block_number ASC), last_block.block) - evt_block_number) * new_gro as "gro_emitted",
            contract_address,
            version,
            evt_tx_hash
        FROM gro_per_block,
             last_block
    )
    
SELECT
    evt_block_time,
    new_gro,
    gro_emitted,
    SUM(COALESCE(gro_emitted, 0)) OVER (ORDER BY evt_block_number ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) as "gro_total",
    version,
    contract_address,
    evt_block_number,
    evt_tx_hash
FROM gro_emitted
ORDER BY evt_block_number DESC

