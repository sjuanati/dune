/*
- @dev:
    - txns earlier than 2021.11.11 are not part of the current blockchain and must be retrieved from legacy tables.
        https://docs.optimism.io/builders/tools/monitor/regenesis-history
    - list of bridged tokens:
        https://docs.optimism.io/chain/tokenlist
*/

WITH
    asset_type AS (
        SELECT asset, address, category
        FROM (VALUES
            ('USDT', 0x94b008aa00579c1307b0ef2c499ad98a8ce58e58, 'fiat-backed'),
            ('USDC', 0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85, 'fiat-backed'),
            ('USDC.e', 0x7f5c764cbc14f9669b88837ca1490cca17c31607, 'fiat-backed'),
            ('sUSD', 0x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9, 'crypto-backed'),
            ('DAI', 0xda10009cbd5d07dd0cecc66161fc93d7c9000da1, 'crypto-backed')
        ) AS t(asset, address, category)
    ),
    period_range AS (
        SELECT asset, dt_seq, 0 AS amount, 0 AS volume, 0 AS txn_count
        FROM asset_type
        CROSS JOIN
        UNNEST(SEQUENCE(DATE '2020-01-01', CURRENT_DATE, INTERVAL '1' DAY)) AS t(dt_seq)
    ),
    usdt AS (
        SELECT
            'USDT' as asset,
            DATE(evt_block_time) AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN value
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -value
                END)
            / 1e6 AS amount,
            SUM(value) / 1e6 AS volume,
            COUNT(*) AS txn_count
        FROM usdt_optimism.USDT_evt_Transfer
        GROUP BY 1, 2
        UNION ALL
        SELECT
            'USDT' as asset,
            DATE(block_time) AS dt,
            SUM(
                CASE
                    WHEN varbinary_ltrim(topic1) = 0x THEN varbinary_to_decimal(varbinary_ltrim(data)) -- from
                    WHEN varbinary_ltrim(topic2) = 0x THEN -varbinary_to_decimal(varbinary_ltrim(data)) -- to
                END)
            / 1e6 AS amount,
            SUM(varbinary_to_decimal(varbinary_ltrim(data))) / 1e6 AS volume,
            COUNT(*) AS txn_count
        FROM optimism_legacy_ovm1.logs
        WHERE contract_address = 0x94b008aa00579c1307b0ef2c499ad98a8ce58e58 -- USDT
        AND topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef -- Transfer
        GROUP BY 1, 2
    ),
    usdc AS (
        SELECT
            'USDC' as asset,
            DATE(evt_block_time) AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN value
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -value
                END)
            / 1e6 AS amount,
            SUM(value) / 1e6 AS volume,
            COUNT(*) AS txn_count
        FROM optimism_usdc_optimism.FiatTokenV2_1_evt_Transfer
        GROUP BY 1, 2
    ),
    usdc_e AS (
        SELECT
            'USDC.e' as asset,
            DATE(evt_block_time) AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN value
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -value
                END)
            / 1e6 AS amount,
            SUM(value) / 1e6 AS volume,
            COUNT(*) AS txn_count
        FROM erc20_optimism.evt_transfer
        WHERE contract_address = 0x7F5c764cBc14f9669B88837ca1490cCa17c31607 -- USDC.e
        GROUP BY 1, 2
        UNION ALL
        SELECT
            'USDC.e' as asset,
            DATE(block_time) AS dt,
            SUM(
                CASE
                    WHEN varbinary_ltrim(topic1) = 0x THEN varbinary_to_decimal(varbinary_ltrim(data)) -- from
                    WHEN varbinary_ltrim(topic2) = 0x THEN -varbinary_to_decimal(varbinary_ltrim(data)) -- to
                END)
            / 1e6 AS amount,
            SUM(varbinary_to_decimal(varbinary_ltrim(data))) / 1e6 AS volume,
            COUNT(*) AS txn_count
        FROM optimism_legacy_ovm1.logs
        WHERE contract_address = 0x7f5c764cbc14f9669b88837ca1490cca17c31607 -- USDC.e
        AND topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef -- Transfer
        GROUP BY 1, 2
    ),
    susd AS (
        SELECT
            'sUSD' as asset,
            DATE(evt_block_time) AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN value
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -value
                END)
            / 1e18 AS amount,
            SUM(value) / 1e18 AS volume,
            COUNT(*) AS txn_count
        FROM erc20_optimism.evt_transfer
        WHERE contract_address = 0x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9 -- sUSD
        GROUP BY 1, 2
        UNION ALL
        SELECT
            'sUSD' as asset,
            DATE(block_time) AS dt,
            SUM(
                CASE
                    WHEN varbinary_ltrim(topic1) = 0x THEN varbinary_to_decimal(varbinary_ltrim(data)) -- from
                    WHEN varbinary_ltrim(topic2) = 0x THEN -varbinary_to_decimal(varbinary_ltrim(data)) -- to
                END)
            / 1e18 AS amount,
            SUM(varbinary_to_decimal(varbinary_ltrim(data))) / 1e18 AS volume,
            COUNT(*) AS txn_count
        FROM optimism_legacy_ovm1.logs
        WHERE contract_address = 0x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9 -- sUSD
        AND topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef -- Transfer
        GROUP BY 1, 2
        UNION ALL
        -- Estimated amount before the start of legacy data
        SELECT
            'sUSD' as asset,
            DATE '2021-06-21' AS dt,
            6000000 AS amount,
            6000000 AS volume, --ish
            1 AS txn_count
    ),
    dai AS (
        SELECT
            'DAI' as asset,
            DATE(evt_block_time) AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN value
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -value
                END)
            / 1e18 AS amount,
            SUM(value) / 1e18 AS volume,
            COUNT(*) AS txn_count
        FROM maker_optimism.L2Dai_evt_Transfer
        GROUP BY 1, 2
        UNION ALL
        SELECT
            'DAI' as asset,
            DATE(block_time) AS dt,
            SUM(
                CASE
                    WHEN varbinary_ltrim(topic1) = 0x THEN varbinary_to_decimal(varbinary_ltrim(data)) -- from
                    WHEN varbinary_ltrim(topic2) = 0x THEN -varbinary_to_decimal(varbinary_ltrim(data)) -- to
                END)
            / 1e18 AS amount,
            SUM(varbinary_to_decimal(varbinary_ltrim(data))) / 1e18 AS volume,
            COUNT(*) AS txn_count
        FROM optimism_legacy_ovm1.logs
        WHERE contract_address = 0xda10009cbd5d07dd0cecc66161fc93d7c9000da1 -- DAI
        AND topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef -- Transfer
        GROUP BY 1, 2
    ),
    combined_data AS (
        SELECT
            asset,
            dt,
            SUM(amount) AS amount,
            SUM(volume) AS volume,
            SUM(txn_count) AS txn_count
        FROM (
            SELECT * FROM usdt
            UNION ALL
            SELECT * FROM usdc
            UNION ALL
            SELECT * FROM usdc_e
            UNION ALL
            SELECT * FROM susd
            UNION ALL
            SELECT * FROM dai
            UNION ALL
            SELECT * from period_range
        )
        GROUP BY 1, 2
    ),
    balances AS (
        SELECT
            COALESCE(at.address, 0x0000000000000000000000000000000000000000) AS address,
            cd.asset,
            at.category,
            cd.dt,
            cd.txn_count AS txn_count,
            cd.volume,
            SUM(cd.amount) OVER (PARTITION BY cd.asset ORDER BY cd.dt) AS balance,
            SUM(cd.amount) OVER (ORDER BY cd.dt) AS balance_total
        FROM combined_data cd
        LEFT JOIN asset_type at
            ON cd.asset = at.asset
    ),
    balances_tt AS (
        SELECT
            address,
            asset,
            category,
            CONCAT(
                '<a href="https://optimistic.etherscan.io/token/',
                CAST(address AS varchar),
                '" target="_blank">🔗</a> ',
                asset
            ) AS asset_link,
            dt,
            txn_count,
            balance,
            balance_total,
            volume,
            COALESCE(LAG(volume, 7) OVER (PARTITION BY asset ORDER BY dt), 0) AS volume_7d_ago,
            COALESCE(LAG(volume, 30) OVER (PARTITION BY asset ORDER BY dt), 0) AS volume_30d_ago,
            CASE 
                WHEN LAG(volume, 7) OVER (PARTITION BY asset ORDER BY dt) = 0 THEN 0
                ELSE (volume - LAG(volume, 7) OVER (PARTITION BY asset ORDER BY dt))
                     / LAG(volume, 7) OVER (PARTITION BY asset ORDER BY dt)
            END AS volume_pct_7d,
            CASE 
                WHEN LAG(volume, 30) OVER (PARTITION BY asset ORDER BY dt) = 0 THEN 0
                ELSE (volume - LAG(volume, 30) OVER (PARTITION BY asset ORDER BY dt))
                     / LAG(volume, 30) OVER (PARTITION BY asset ORDER BY dt)
            END AS volume_pct_30d,
            COALESCE(LAG(balance, 7) OVER (PARTITION BY asset ORDER BY dt), 0) AS balance_7d_ago,
            COALESCE(LAG(balance, 30) OVER (PARTITION BY asset ORDER BY dt), 0) AS balance_30d_ago,
            CASE 
                WHEN LAG(balance, 7) OVER (PARTITION BY asset ORDER BY dt) = 0 THEN 0
                ELSE (balance - LAG(balance, 7) OVER (PARTITION BY asset ORDER BY dt))
                     / LAG(balance, 7) OVER (PARTITION BY asset ORDER BY dt)
            END AS balance_pct_7d,
            CASE 
                WHEN LAG(balance, 30) OVER (PARTITION BY asset ORDER BY dt) = 0 THEN 0
                ELSE (balance - LAG(balance, 30) OVER (PARTITION BY asset ORDER BY dt))
                     / LAG(balance, 30) OVER (PARTITION BY asset ORDER BY dt)
            END AS balance_pct_30d
        FROM balances
    )
    
SELECT *
FROM balances_tt
WHERE dt > date '2020-01-01'
ORDER BY dt ASC, balance DESC