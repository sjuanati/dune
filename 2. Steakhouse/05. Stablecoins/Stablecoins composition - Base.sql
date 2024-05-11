
WITH
    asset_type AS (
        SELECT asset, address, category
        FROM (VALUES
            ('USDC', 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913, 'fiat-backed'),
            ('DOLA', 0x4621b7A9c75199271F773Ebd9A499dbd165c3191, 'crypto-backed'),
            ('USD+', 0xB79DD08EA68A908A97220C76d19A6aA9cBDE4376, 'crypto-backed')
        ) AS t(asset, address, category)
    ),
    period_range AS (
        SELECT asset, dt_seq, 0 AS amount, 0 AS volume, 0 AS txn_count
        FROM asset_type
        CROSS JOIN
        UNNEST(SEQUENCE(DATE '2020-01-01', CURRENT_DATE, INTERVAL '1' DAY)) AS t(dt_seq)
    ),
    usdc AS (
        SELECT
            'USDC' as asset,
            block_date AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN amount
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -amount
                END)
            AS amount,
            SUM(amount) AS volume,
            COUNT(*) AS txn_count
        FROM tokens_base.transfers
        WHERE contract_address = 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913 -- USDC
        GROUP BY 1, 2
    ),
    dola AS (
        SELECT
            'DOLA' as asset,
            block_date AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN amount
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -amount
                END)
            AS amount,
            SUM(amount) AS volume,
            COUNT(*) AS txn_count
        FROM tokens_base.transfers
        WHERE contract_address = 0x4621b7A9c75199271F773Ebd9A499dbd165c3191 -- DOLA
        GROUP BY 1, 2
    ),
    usd_plus AS (
        SELECT
            'USD+' as asset,
            block_date AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN amount
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -amount
                END)
            AS amount,
            SUM(amount) AS volume,
            COUNT(*) AS txn_count
        FROM tokens_base.transfers
        WHERE contract_address = 0xB79DD08EA68A908A97220C76d19A6aA9cBDE4376 -- USD+
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
            SELECT * FROM dola
            UNION ALL
            SELECT * FROM usd_plus
            UNION ALL
            SELECT * FROM period_range
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
                '<a href="https://basescan.org/token/',
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