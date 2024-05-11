/*
https://tron-converter.com/
USDT: TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t -> 0xa614f803B6FD780986A42c78Ec9c7f77e6DeD13C
USDC: TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8 -> 0x3487b63D30B5B2C87fb7fFa8bcfADE38EAaC1abe
USDD: TPYmHEhy5n8TCEfYGqW2rPxsghSfzghPDn -> 0x94F24E992cA04B49C6f2a2753076Ef8938eD4daa
TUSD: TUpMhErZL2fhh4sVNULAbNKLokS4GjC1F4 -> 0xcEbDE71077b830B958C8da17bcddeeB85D0BCf25
USDJ: TMwFHYXLJaRUPeW6421aqXL4ZEzPRFGkGT -> 0x834295921A488D9d42b4b3021ED1a3C39fB0f03e
*/

WITH
    asset_type AS (
        SELECT asset, address, raw_address, category
        FROM (VALUES
            ('USDT', 0xa614f803b6fd780986a42c78ec9c7f77e6ded13c, 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t', 'fiat-backed'),
            ('USDC', 0x3487b63D30B5B2C87fb7fFa8bcfADE38EAaC1abe, 'TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8', 'fiat-backed'),
            ('USDD', 0x94F24E992cA04B49C6f2a2753076Ef8938eD4daa, 'TPYmHEhy5n8TCEfYGqW2rPxsghSfzghPDn', 'algorithmic'),
            ('TUSD', 0xcEbDE71077b830B958C8da17bcddeeB85D0BCf25, 'TUpMhErZL2fhh4sVNULAbNKLokS4GjC1F4', 'fiat-backed'),
            ('USDJ', 0x834295921A488D9d42b4b3021ED1a3C39fB0f03e, 'TMwFHYXLJaRUPeW6421aqXL4ZEzPRFGkGT', 'crypto-backed')
        ) AS t(asset, address, raw_address, category)
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
            evt_block_date AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN value
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -value
                END)
            / 1e6 AS amount,
            SUM(value) / 1e6 AS volume,
            COUNT(*) AS txn_count
        FROM tether_tron.Tether_USD_evt_Transfer
        GROUP BY 1, 2
    ),
    usdc AS (
        SELECT
            'USDC' as asset,
            evt_block_date AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN value
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -value
                END)
            / 1e6 AS amount,
            SUM(value) / 1e6 AS volume,
            COUNT(*) AS txn_count
        FROM erc20_tron.evt_transfer
        WHERE contract_address = 0x3487b63D30B5B2C87fb7fFa8bcfADE38EAaC1abe -- USDC
        AND evt_block_date > DATE '2021-06-08'
        GROUP BY 1, 2
    ),
    usdd AS (
        SELECT
            'USDD' as asset,
            evt_block_date AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN value
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -value
                END)
            / 1e18 AS amount,
            SUM(value) / 1e18 AS volume,
            COUNT(*) AS txn_count
        FROM erc20_tron.evt_transfer
        WHERE contract_address = 0x94F24E992cA04B49C6f2a2753076Ef8938eD4daa -- USDD
        AND evt_block_date > DATE '2022-04-29'
        GROUP BY 1, 2
    ),
    tusd AS (
        SELECT
            'TUSD' as asset,
            evt_block_date AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN value
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -value
                END)
            / 1e18 AS amount,
            SUM(value) / 1e18 AS volume,
            COUNT(*) AS txn_count
        FROM erc20_tron.evt_transfer
        WHERE contract_address = 0xcEbDE71077b830B958C8da17bcddeeB85D0BCf25 -- TUSD
        AND evt_block_date > DATE '2021-03-22'
        GROUP BY 1, 2
    ),
    usdj AS (
        SELECT
            'USDJ' as asset,
            evt_block_date AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN value
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -value
                END)
            / 1e18 AS amount,
            SUM(value) / 1e18 AS volume,
            COUNT(*) AS txn_count
        FROM erc20_tron.evt_transfer
        WHERE contract_address = 0x834295921A488D9d42b4b3021ED1a3C39fB0f03e -- USDJ
        AND evt_block_date > DATE '2020-04-02'
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
            SELECT * FROM usdd
            UNION ALL
            SELECT * FROM tusd
            UNION ALL
            SELECT * FROM usdj
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
            at.raw_address,
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
                '<a href="https://tronscan.org/#/token20/',
                raw_address,
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