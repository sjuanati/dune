WITH
    airdrops AS (
        SELECT
            evt_block_time,
            CAST(totalAmount  AS DOUBLE) / 1e18 as "gro_airdropped",
            trancheId,
            merkleRoot,
            'v1' as "version",
            contract_address,
            evt_block_number,
            evt_tx_hash
        FROM gro_ethereum.AirDrop_evt_LogNewDrop
        UNION ALL
        SELECT
            evt_block_time,
            CAST(totalAmount  AS DOUBLE) / 1e18 as "gro_airdropped",
            trancheId,
            merkleRoot,
            'v2' as "version",
            contract_address,
            evt_block_number,
            evt_tx_hash
        FROM gro_ethereum.AirDropV2_evt_LogNewDrop
    )
SELECT
    evt_block_time,
    gro_airdropped,
    SUM(COALESCE(gro_airdropped, 0)) OVER (ORDER BY evt_block_number ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) as "gro_total",
    trancheId,
    merkleRoot,
    version,
    contract_address,
    evt_block_number,
    evt_tx_hash
FROM airdrops
ORDER BY evt_block_number DESC
