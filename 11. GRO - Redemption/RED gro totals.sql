WITH
    gro_liquid AS (
        SELECT SUM(amount) AS "amount" 
        FROM (
            SELECT
                CASE
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -CAST("value" AS DOUBLE)
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN CAST("value" AS DOUBLE)
                END / 1e18 AS amount
            FROM gro_ethereum.GROToken_evt_Transfer
            WHERE evt_block_number <= 18428468 -- redemption deadline block
        )
    ),
    gro_price AS (
        SELECT (CAST(reserve1 AS DOUBLE) / 1e6) / (CAST(reserve0 AS DOUBLE) / 1e18) as "value"
        FROM  uniswap_v2_ethereum.Pair_evt_Sync
        WHERE contract_address = 0x21c5918ccb42d20a2368bdca8feda0399ebfd2f6
        ORDER BY evt_block_number DESC
        LIMIT 1
    ),
    -- Calculate total cUSDC deposited in redemption
    cusdc_deposited AS (
        SELECT COALESCE(SUM(CAST("amount" AS DOUBLE) / 1e8), 0) * 0.0231 AS "value"
        FROM gro_ethereum.RedemptionPool_evt_CUSDCDeposit
        WHERE evt_block_number <= 18428468 -- redemption deadline block
    )

SELECT
    gl."amount" AS "gro_liquid",
    gl."amount" AS "total_gro_redeemable",
    p."value",
    cp."value" / (gl."amount") AS "gro_estimated_value"
FROM
    gro_liquid gl,
    gro_price p,
    cusdc_deposited cp