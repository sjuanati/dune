WITH
    asset_type AS (
        SELECT asset, address, address_sol, category
        FROM (VALUES
            --filed 'address' only valid to join with prices table
            ('USDC', 0xc6fa7af3bedbad3a3d65f36aabc97431b1bbe4c2d2f6e0e47ca60203452f5d61, 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 'fiat-backed'),
            ('USDT', 0xce010e60afedb22717bd63192f54145a3f965a33bb82d2c7029eb2ce1e208264, 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', 'fiat-backed'),
            ('PYUSD', 0x0000000000000000000000000000000000000000000000000000000000000000, '2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo', 'fiat-backed')
        ) AS t(asset, address, address_sol, category)
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
            DATE(block_time) AS dt,
            SUM(
                CASE
                    WHEN
                        action = 'mint' 
                        AND to_owner is not null -- exclude pre-minting
                        AND to_owner != '7VHUFJHWu2CuExkJcJrzhQPJ2oygupTWkL2A2For4BmE' -- exclude mints to Circle
                        THEN amount  
                    WHEN
                        action = 'burn' 
                        AND from_owner != '7VHUFJHWu2CuExkJcJrzhQPJ2oygupTWkL2A2For4BmE' -- exclude burns from Circle
                        THEN -amount
                    WHEN
                        action = 'transfer' 
                        AND to_owner = '7VHUFJHWu2CuExkJcJrzhQPJ2oygupTWkL2A2For4BmE' -- exclude transfer to Circle
                        THEN -amount
                    WHEN
                        action = 'transfer' 
                        AND from_owner = '7VHUFJHWu2CuExkJcJrzhQPJ2oygupTWkL2A2For4BmE' -- include transfers from Circle
                        THEN amount
                END)
            / 1e6 AS amount,
            SUM(amount) / 1e6 AS volume,
            COUNT(*) AS txn_count
        FROM tokens_solana.transfers
        WHERE token_mint_address = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v' --USDC
        AND block_date > DATE '2020-10-15'
        GROUP BY 1, 2
    ),
    others AS (
        SELECT
            a.asset as asset,-- 'USDT' as asset,
            DATE(block_time) AS dt,
            SUM(
                CASE
                    WHEN
                        action = 'mint' 
                        AND to_owner is not null -- exclude pre-minting
                        THEN amount  
                    WHEN
                        action = 'burn' 
                        THEN -amount
                END)
            / 1e6 AS amount,
            SUM(amount) / 1e6 AS volume,
            COUNT(*) AS txn_count
        FROM tokens_solana.transfers t
        LEFT JOIN asset_type a
            ON t.token_mint_address = a.address_sol
        WHERE token_mint_address in (
            'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', --USDT
            '2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo' -- PYUSD
        )
        AND block_date > DATE '2021-02-01'
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
            SELECT * FROM others
            UNION ALL
            SELECT * FROM period_range
        )
        GROUP BY 1, 2
    ),
    balances AS (
        SELECT
            COALESCE(at.address, 0x0000000000000000000000000000000000000000) AS address,
            address_sol,
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
                '<a href="https://solscan.io/token/',
                CAST(address_sol AS varchar),
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
        
        
        
