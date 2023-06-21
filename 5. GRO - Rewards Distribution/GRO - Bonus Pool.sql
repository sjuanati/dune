/*
/// @title Bonus Pool
/// @db_engine: v2 Dune SQL
/// @purpose Provide the GRO rewards distributed through Vesting Bonus Pool (via GROHodler contract)
/// @contracts:
///     Hodler v1: 0xEf10eac205817A88C6d504d02481053E85A8F927
///     Hodler v2: 0x8b4A30c8884ca4AfF1E4c82Cce79802a63E61397
///     Hodler v3: 0x7C268Bf50e64258835029c30C91DaA65a9E55b5a
*/

WITH
    bonus AS (
        SELECT
            "evt_block_time" AS "current_date",
            "evt_tx_hash",
            "contract_address",
            CAST("amount" AS DOUBLE) / 1e18 AS "amount"
        FROM gro_ethereum.GROHodler_evt_LogBonusAdded
        UNION ALL
        SELECT
            "evt_block_time" AS "current_date",
            "evt_tx_hash",
            "contract_address",
            CAST("amount" AS DOUBLE) / 1e18 AS "amount"
        FROM gro_ethereum.GROHodlerV2_evt_LogBonusAdded
        UNION ALL
        SELECT
            "evt_block_time" AS "current_date",
            "evt_tx_hash",
            "contract_address",
            CAST("amount" AS DOUBLE) / 1e18 AS "amount"
        FROM gro_ethereum.GROHodlerV3_evt_LogBonusAdded
    )

SELECT
    "current_date",
    "amount",
    SUM("amount") OVER (ORDER BY "current_date" ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) AS "acc_amount",
    "contract_address",
    "evt_tx_hash"
FROM bonus
GROUP BY 1,2,4,5
ORDER BY 1 DESC
