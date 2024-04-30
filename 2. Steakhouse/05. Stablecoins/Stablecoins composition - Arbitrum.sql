WITH
    asset_type AS (
        SELECT asset, address
        FROM (VALUES
            ('USDC', 0xaf88d065e77c8cC2239327C5EDb3A432268e5831),
            ('USDT', 0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9),
            ('DAI', 0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1),
            ('FRAX', 0x17FC002b466eEc40DaE837Fc4bE5c67993ddBd6F),
            ('MIM', 0xFEa7a6a0B346362BF88A9e4A88416B77a57D6c2A)
        ) AS t(asset, address)
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
            DATE(block_date) AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN amount
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -amount
                END)
            AS amount,
            SUM(amount) AS volume,
            COUNT(*) AS txn_count
        FROM tokens_arbitrum.transfers
        WHERE contract_address = 0xaf88d065e77c8cC2239327C5EDb3A432268e5831 -- USDC
        AND block_date > date '2022-10-30'
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
        FROM tether_arbitrum.ArbitrumExtension_evt_Transfer
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
        FROM tokens_arbitrum.transfers
        WHERE contract_address = 0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1 -- DAI
        AND block_date > date '2021-09-19'
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
        FROM tokens_arbitrum.transfers
        WHERE contract_address = 0x17FC002b466eEc40DaE837Fc4bE5c67993ddBd6F -- FRAX
        AND block_date > date '2021-09-26'
        GROUP BY 1, 2
    ),
    mim AS (
        SELECT
            'MIM' as asset,
            DATE(block_date) AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN amount
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -amount
                END)
            AS amount,
            SUM(amount) AS volume,
            COUNT(*) AS txn_count
        FROM tokens_arbitrum.transfers
        WHERE contract_address = 0xFEa7a6a0B346362BF88A9e4A88416B77a57D6c2A -- MIM
        AND block_date > date '2021-09-13'
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
            SELECT * FROM usdt
            UNION ALL
            SELECT * FROM dai
            UNION ALL
            SELECT * FROM frax
            UNION ALL
            SELECT * FROM mim
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