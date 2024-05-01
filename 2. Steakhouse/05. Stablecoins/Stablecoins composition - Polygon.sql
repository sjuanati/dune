WITH
    asset_type AS (
        SELECT asset, address
        FROM (VALUES 
            ('USDC', 0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359), -- + usdc_pos = 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174
            ('USDT', 0xc2132d05d31c914a87c6611c10748aeb04b58e8f),
            ('DAI', 0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063),
            ('FRAX', 0x45c32fA6DF82ead1e2EF74d17b76547EDdFaFF89),
            ('EURS', 0xe111178a87a3bff0c8d18decba5798827539ae99)
        ) AS t(asset, address)
    ),
    period_range AS (
        SELECT asset, dt_seq, 0 AS amount, 0 as volume, 0 AS txn_count
        FROM asset_type
        CROSS JOIN
        UNNEST(SEQUENCE(DATE '2020-01-01', CURRENT_DATE, INTERVAL '1' DAY)) AS t(dt_seq)
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
        FROM usdc_polygon.FiatTokenV2_1_evt_Transfer
        GROUP BY 1, 2
    ),
    usdc_pos AS (
        SELECT
            'USDC' as asset,
            DATE(block_date) AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN amount
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -amount
                END)
            AS amount,
            SUM(amount) AS volume,
            COUNT(*) AS txn_count
        FROM tokens_polygon.transfers
        WHERE contract_address = 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174 -- USDC PoS
        AND block_date > date '2020-09-26'
        GROUP BY 1, 2
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
        FROM tether_polygon.UChildERC20_evt_Transfer
        GROUP BY 1, 2
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
        FROM maker_polygon.dai_evt_Transfer
        GROUP BY 1, 2
    ),
    frax AS (
        SELECT
            'FRAX' as asset,
            DATE(block_date) AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN amount
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -amount
                END)
            AS amount,
            SUM(amount) AS volume,
            COUNT(*) AS txn_count
        FROM tokens_polygon.transfers
        WHERE contract_address = 0x45c32fA6DF82ead1e2EF74d17b76547EDdFaFF89
        AND block_date > date '2021-09-20'
        GROUP BY 1, 2
    ),
    eurs AS (
        SELECT
            'EURS' as asset,
            DATE(evt_block_time) AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN value
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -value
                END)
            / 1e2 AS amount,
            SUM(value) / 1e2 AS volume,
            COUNT(*) AS txn_count
        FROM stasis_polygon.EURS_evt_Transfer
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
            SELECT * FROM usdc
            UNION ALL
            SELECT * FROM usdc_pos
            UNION ALL
            SELECT * FROM usdt
            UNION ALL
            SELECT * FROM dai
            UNION ALL
            SELECT * FROM frax
            UNION ALL
            SELECT * FROM eurs
            UNION ALL
            SELECT * FROM period_range
        )
        GROUP BY 1, 2
    ),
    balances AS (
        SELECT
            COALESCE(at.address, 0x0000000000000000000000000000000000000000) AS address,
            cd.asset,
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
            CONCAT(
                '<a href="https://etherscan.io/token/',
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