WITH
    asset_type AS (
        SELECT asset, address
        FROM (VALUES
            ('USDT', 0x55d398326f99059fF775485246999027B3197955),
            ('USDC', 0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d),
            ('BUSD', 0xe9e7cea3dedca5984780bafc599bd69add087d56),
            ('FDUSD', 0xc5f0f7b66764F6ec8C8Dff7BA683102295E16409),
            ('DAI', 0x1AF3F329e8BE154074D8769D1FFa4eE058B1DBc3)
        ) AS t(asset, address)
    ),
    period_range AS (
        SELECT asset, dt_seq, 0 AS amount, 0 as volume, 0 AS txn_count
        FROM asset_type
        CROSS JOIN
        UNNEST(SEQUENCE(DATE '2020-01-01', CURRENT_DATE, INTERVAL '1' DAY)) AS t(dt_seq)
    ),
    usdt AS (
        SELECT
            'USDT' as asset,
            DATE(block_date) AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN amount
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -amount
                END)
            AS amount,
            SUM(amount) AS volume,
            COUNT(*) AS txn_count
        FROM tokens_bnb.transfers
        WHERE contract_address = 0x55d398326f99059fF775485246999027B3197955 -- USDT
        AND block_date > date '2020-09-03'
        GROUP BY 1, 2
    ),
    usdc AS (
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
        FROM tokens_bnb.transfers
        WHERE contract_address = 0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d -- USDC
        AND block_date > date '2020-10-18'
        GROUP BY 1, 2
    ),
    busd AS (
        SELECT
            'BUSD' as asset,
            DATE(block_date) AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN amount
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -amount
                END)
            AS amount,
            SUM(amount) AS volume,
            COUNT(*) AS txn_count
        FROM tokens_bnb.transfers
        WHERE contract_address = 0xe9e7cea3dedca5984780bafc599bd69add087d56 -- BUSD
        AND block_date > date '2020-09-01'
        GROUP BY 1, 2
    ),
    fdusd AS (
        SELECT
            'FDUSD' as asset,
            DATE(block_date) AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN amount
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -amount
                END)
            AS amount,
            SUM(amount) AS volume,
            COUNT(*) AS txn_count
        FROM tokens_bnb.transfers
        WHERE contract_address = 0xc5f0f7b66764F6ec8C8Dff7BA683102295E16409 -- FDUSD
        AND block_date > date '2023-05-01'
        GROUP BY 1, 2
    ),
    dai AS (
        SELECT
            'DAI' as asset,
            DATE(block_date) AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN amount
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -amount
                END)
            AS amount,
            SUM(amount) AS volume,
            COUNT(*) AS txn_count
        FROM tokens_bnb.transfers
        WHERE contract_address = 0x1AF3F329e8BE154074D8769D1FFa4eE058B1DBc3 -- DAI
        AND block_date > date '2020-09-08'
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
            SELECT * FROM busd
            UNION ALL
            SELECT * FROM fdusd
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