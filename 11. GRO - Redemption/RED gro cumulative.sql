WITH
    gro AS (
        SELECT
            date_trunc('week', "evt_block_time") AS "date",
            CASE
                WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -CAST("value" AS DOUBLE)
                WHEN "from" = 0x0000000000000000000000000000000000000000 THEN CAST("value" AS DOUBLE)
            END / 1e18 AS amount
        FROM gro_ethereum.GROToken_evt_Transfer
    ),
    gro_total AS (
        SELECT SUM(amount) AS "amount" FROM gro
    ),
    weekly_gro AS (
        SELECT
            "date",
            SUM(amount) AS "amount"
        FROM gro
        GROUP BY "date"
        ORDER BY "date" ASC
    ),
    gro_cumulative AS (
        SELECT
            "date",
            SUM(COALESCE(amount, 0)) OVER (ORDER BY "date" ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) as "amount"
        FROM weekly_gro
    ),
    gro_per_user AS (
        SELECT 
            "user",
            sum("amount") AS "amount"
        FROM (
            SELECT
                "to" AS "user",
                SUM(CAST("value" AS DOUBLE) / 1e18) AS amount
            FROM gro_ethereum.GROToken_evt_Transfer
            WHERE "to" != 0x0000000000000000000000000000000000000000
            GROUP BY 1
            UNION ALL
            SELECT
                "from" AS "user",
                SUM(-CAST("value" AS DOUBLE) / 1e18 ) AS amount
            FROM gro_ethereum.GROToken_evt_Transfer
            WHERE "from" != 0x0000000000000000000000000000000000000000
            GROUP BY 1
        )
        GROUP BY 1
    ),
    gro_price AS (
        SELECT (CAST(reserve1 AS DOUBLE) / 1e6) / (CAST(reserve0 AS DOUBLE) / 1e18) as "value"
        FROM  uniswap_v2_ethereum.Pair_evt_Sync
        WHERE contract_address = 0x21c5918ccb42d20a2368bdca8feda0399ebfd2f6
        ORDER BY evt_block_number DESC
        LIMIT 1
    )

SELECT
    concat(
        '<a href="https://etherscan.io/address/', 
        cast(gpu."user" as varchar),
        '" target="_blank" >',
        cast(gpu."user" as varchar),
        '</a>'
    ) AS "user",
    CASE
        WHEN gpu."user" = 0x359f4fe841f246a095a82cb26f5819e10a91fe0d THEN 'GRO Treasury'
        WHEN gpu."user" = 0x21C5918CcB42d20A2368bdCA8feDA0399EbfD2f6 THEN 'Uniswap V2 Pool GRO/USDC'
        WHEN gpu."user" = 0x2ac5bC9ddA37601EDb1A5E29699dEB0A5b67E9bB THEN 'Uniswap V2 Pool GVT/GRO'
        WHEN gpu."user" = 0xBA12222222228d8Ba445958a75a0704d566BF2C8 THEN 'Balancer V2 Pool GRO/WETH'
        WHEN gpu."user" = 0x2E32bAd45a1C29c1EA27cf4dD588DF9e68ED376C THEN 'GRO LP Token Staker'
        WHEN gpu."user" = 0x378Ba9B73309bE80BF4C2c027aAD799766a7ED5A THEN 'Votium Multi Merkle Stash'
        ELSE 'GRO Hodler'
    END AS "type",
    gpu."amount" AS "gro_amount",
    gt."amount" AS "gro_total_amount",
    gpu."amount" * p."value" as "gro_value",
    p."value"
FROM gro_per_user gpu, gro_total gt, gro_price p
WHERE gpu."amount" > 0.01
ORDER BY 3 DESC
