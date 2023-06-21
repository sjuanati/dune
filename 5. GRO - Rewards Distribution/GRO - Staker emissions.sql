/*
/// @title Staker emissions
/// @db_engine: v2 Dune SQL
/// @purpose Provide the GRO rewards emitted through the Sushiswap's MasterChef (aka. LpTokenStaker) via the different Liquidity Pools
/// @contracts:
///     LpTokenStaker v1: 0x001C249c09090D79Dc350A286247479F08c7aaD7
///     LpTokenStaker v2: 0x2E32bAd45a1C29c1EA27cf4dD588DF9e68ED376C
*/
WITH
    gro_per_block AS (
        SELECT
            CAST("newGro" AS DOUBLE) / 1e18 AS "new_gro",
            'v1' AS "version",
            "evt_block_time",
            "evt_block_number"
        FROM gro_ethereum.LPTokenStaker_evt_LogGroPerBlock
        UNION ALL
        SELECT
            CAST("newGro" AS DOUBLE) / 1e18 AS "new_gro",
            'v2' AS "version",
            "evt_block_time",
            "evt_block_number"
        FROM gro_ethereum.LPTokenStakerV2_evt_LogGroPerBlock
    ),
    last_block AS (
        SELECT MAX("number") AS "block" FROM ethereum.blocks
    ),
    gro_emitted AS (
        SELECT
            "evt_block_time" AS "from_date",
            LEAD("evt_block_time", 1, current_timestamp) OVER (ORDER BY "evt_block_number" ASC) AS "to_date",
            "new_gro",
            "evt_block_number",
            (COALESCE(LEAD("evt_block_number", 1) OVER (ORDER BY "evt_block_number" ASC), last_block."block") - "evt_block_number") * "new_gro" AS "gro_emitted",
            "version"
        FROM gro_per_block,
             last_block
    )
    
SELECT
    "from_date",
    "to_date",
    "new_gro",
    "gro_emitted",
    SUM(COALESCE("gro_emitted", 0)) OVER (ORDER BY "evt_block_number" ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) AS "gro_total",
    "version" AS "staker"
FROM gro_emitted
ORDER BY evt_block_number DESC
