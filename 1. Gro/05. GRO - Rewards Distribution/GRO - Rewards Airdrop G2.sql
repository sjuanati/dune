/*
/// @title Rewards airdrop
/// @db_engine: v2 Dune SQL
/// @purpose Provide the GRO rewards distributed through airdrops after snapshot approval
/// @contracts:
///     Airdrop v1: 0x6b1bff72f00cc147b5dc7a5b156fe7a6fd206dda
///     Airdrop v2: 0xf3d39a7feba9be0c1d18b355e7ed01070ee2c561
*/

WITH
    airdrops AS (
        SELECT
            "evt_block_time",
            CAST("totalAmount" AS DOUBLE) / 1e18 AS "gro_airdropped",
            "trancheId",
            "merkleRoot",
            'v1' as "version",
            "contract_address",
            "evt_block_number",
            "evt_tx_hash"
        FROM gro_ethereum.AirDrop_evt_LogNewDrop
        UNION ALL
        SELECT
            "evt_block_time",
            CAST("totalAmount" AS DOUBLE) / 1e18 AS "gro_airdropped",
            "trancheId",
            "merkleRoot",
            'v2' as "version",
            "contract_address",
            "evt_block_number",
            "evt_tx_hash"
        FROM gro_ethereum.AirDropV2_evt_LogNewDrop
    ),
    claims AS (
        SELECT 
            "trancheId",
            "version",
            SUM(CAST("amount" AS DOUBLE) / 1e18) AS "gro_claimed"
        FROM (
            SELECT "trancheId" AS "trancheId",
                'v1' AS "version",
                "amount"
            FROM gro_ethereum.AirDrop_evt_LogClaim
            UNION ALL
            SELECT "trancheId" AS "trancheId",
                'v2' AS "version",
                "amount"
            FROM gro_ethereum.AirDropV2_evt_LogClaim
        )
        GROUP BY 1,2
)

SELECT
    a."evt_block_time",
    a."gro_airdropped",
    c."gro_claimed",
    SUM(COALESCE(a."gro_airdropped", 0)) OVER (ORDER BY a."evt_block_number" ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) AS "gro_total_airdropped",
    SUM(COALESCE(c."gro_claimed", 0)) OVER (ORDER BY a."evt_block_number" ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) AS "gro_total_claimed",
    a."trancheId",
    a."merkleRoot",
    a."version",
    a."contract_address",
    a."evt_block_number",
    a."evt_tx_hash"
FROM airdrops a
LEFT JOIN claims c
    ON a."trancheId" = c."trancheId"
    AND a."version" = c."version"
ORDER BY a."evt_block_number" DESC
