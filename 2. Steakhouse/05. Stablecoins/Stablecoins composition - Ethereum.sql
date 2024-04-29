/*
@dev:
- sDAI: https://devs.spark.fi/sdai/technical-docs
*/

WITH
    asset_type AS (
        SELECT asset, address
        FROM (VALUES 
            ('USDC', 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),
            ('USDT', 0xdac17f958d2ee523a2206206994597c13d831ec7),
            ('DAI', 0x6b175474e89094c44da98b954eedeac495271d0f),
            ('FRAX', 0x853d955acef822db058eb8505911ed77f175b99e),
            ('USDP', 0x8e870d67f660d95d5be530380d0ec0bd388289e1),
            ('GUSD', 0x056Fd409E1d7A124BD7017459dFEa2F387b6d5Cd),
            ('BUSD', 0x4fabb145d64652a948d72533023f6e7a623c7c53),
            ('TUSD', 0x0000000000085d4780B73119b644AE5ecd22b376),
            ('LUSD', 0x5f98805A4E8be255a32880FDeC7F6728C6568bA0),
            ('FDUSD', 0xc5f0f7b66764F6ec8C8Dff7BA683102295E16409),
            ('PYUSD', 0x6c3ea9036406852006290770bedfcaba0e23a0e8),
            ('crvUSD', 0xf939E0A03FB07F59A73314E73794Be0E57ac1b4E),
            ('USDe', 0x4c9edd5852cd905f086c759e8383e09bff1e68b3),
            ('sDAI', 0x83F20F44975D03b1b09e64809B757c47f942BEeA),
            ('BUIDL', 0x7712c34205737192402172409a8F7ccef8aA2AEc)
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
            DATE(evt_block_time) AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN value
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -value
                END)
            / 1e6 AS amount,
            SUM(value) / 1e6 AS volume,
            COUNT(*) AS txn_count
        FROM circle_ethereum.USDC_evt_Transfer ut
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
        FROM tether_ethereum.Tether_USD_evt_Transfer
        GROUP BY 1, 2
        UNION ALL
        SELECT
            'USDT' as asset,
            DATE(evt_block_time) AS dt,
            SUM(amount) / 1e6 AS amount,
            SUM(amount) / 1e6 AS volume,
            0 AS txn_count
        FROM tether_ethereum.Tether_USD_evt_Issue
        GROUP BY 1, 2
        UNION ALL
        SELECT
            'USDT' as asset,
            DATE(evt_block_time) AS dt,
            - SUM(amount) / 1e6 AS amount,
            SUM(amount) / 1e6 AS volume,
            0 AS txn_count
        FROM tether_ethereum.Tether_USD_evt_Redeem
        GROUP BY 1, 2
    ),
    dai AS (
        SELECT
            'DAI' AS asset,
            DATE(evt_block_time) AS dt,
            SUM(
                CASE
                    WHEN "src" = 0x0000000000000000000000000000000000000000 THEN wad
                    WHEN "dst" = 0x0000000000000000000000000000000000000000 THEN -wad
                END)
            / 1e18 AS amount,
            SUM(wad) / 1e18 AS volume,
            COUNT(*) AS txn_count
        FROM maker_ethereum.DAI_evt_Transfer
        GROUP BY 1, 2
        /*
        UNION ALL
        SELECT
            'DAI' AS asset,
            DATE(call_block_time) AS dt,
            SUM(
                CASE
                    WHEN "src" = 0x197e90f9fad81970ba7976f33cbd77088e5d7cf7 THEN -rad / 1e27
                    WHEN "dst" = 0x197e90f9fad81970ba7976f33cbd77088e5d7cf7 THEN rad / 1e27
                END)
            / 1e18 AS amount
        FROM maker_ethereum.VAT_call_move WHERE call_success
        GROUP BY 1, 2
        UNION ALL
        SELECT
            'DAI' AS asset,
            DATE(call_block_time) AS dt,
            SUM(rad / 1e27) / 1e18 AS amount
        FROM maker_ethereum.VAT_call_suck WHERE call_success AND v = 0x197e90f9fad81970ba7976f33cbd77088e5d7cf7
        GROUP BY 1, 2
        */
    ),
    sdai AS (
        SELECT
            'sDAI' as asset,
            DATE(evt_block_time) AS dt,
            SUM(shares) / 1e18 AS amount,
            SUM(shares) / 1e18 AS volume,
            COUNT(*) AS txn_count
        FROM maker_ethereum.SavingsDai_evt_Deposit
        GROUP BY 1, 2
        UNION ALL
        SELECT
            'sDAI' as asset,
            DATE(evt_block_time) AS dt,
            - SUM(shares) / 1e18 AS amount,
            SUM(shares) / 1e18 AS volume,
            COUNT(*) AS txn_count
        FROM maker_ethereum.SavingsDai_evt_Withdraw
        GROUP BY 1, 2
    ),
    frax AS (
        SELECT
            'FRAX' as asset,
            DATE(evt_block_time) AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN value
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -value
                END)
            / 1e18 AS amount,
            SUM(value) / 1e18 AS volume,
            COUNT(*) AS txn_count
        FROM frax_ethereum.FRAXStablecoin_evt_Transfer
        GROUP BY 1, 2
    ),
    usdp AS (
        SELECT
            'USDP' as asset,
            DATE(evt_block_time) AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN value
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -value
                END)
            / 1e18 AS amount,
            SUM(value) / 1e18 AS volume,
            COUNT(*) AS txn_count
        FROM usdp_ethereum.USDPImplementationV3_evt_Transfer
        GROUP BY 1, 2
    ),
    gusd AS (
        SELECT
            'GUSD' as asset,
            DATE(evt_block_time) AS dt,
            SUM(
                CASE
                    WHEN _from = 0x0000000000000000000000000000000000000000 THEN _value
                    WHEN _to = 0x0000000000000000000000000000000000000000 THEN -_value
                END)
            / 1e2 AS amount,
            SUM(_value) / 1e2 AS volume,
            COUNT(*) AS txn_count
        FROM gemini_ethereum.GUSD_evt_Transfer
        GROUP BY 1, 2
    ),
    busd AS (
        SELECT
            'BUSD' as asset,
            DATE(evt_block_time) AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN value
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -value
                END)
            / 1e18 AS amount,
            SUM(value) / 1e18 AS volume,
            COUNT(*) AS txn_count
        FROM busd_ethereum.BUSDImplementation_evt_Transfer
        GROUP BY 1, 2
    ),
    tusd AS (
        SELECT
            'TUSD' as asset,
            DATE(evt_block_time) AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN value
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -value
                END)
            / 1e18 AS amount,
            SUM(value) / 1e18 AS volume,
            COUNT(*) AS txn_count
        FROM erc20_ethereum.evt_transfer
        WHERE DATE(evt_block_time) > DATE '2018-03-03'
        AND contract_address in (
            0x0000000000085d4780b73119b644ae5ecd22b376, -- new tUSD
            0x8dd5fbCe2F6a956C3022bA3663759011Dd51e73E  -- old tUSD
        )
        GROUP BY 1, 2
    ),
    lusd AS (
        SELECT
            'LUSD' as asset,
            DATE(evt_block_time) AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN value
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -value
                END)
            / 1e18 AS amount,
            SUM(value) / 1e18 AS volume,
            COUNT(*) AS txn_count
        FROM liquity_ethereum.LUSD_evt_Transfer
        GROUP BY 1, 2
    ),
    fdusd AS (
        SELECT
            'FDUSD' as asset,
            DATE(evt_block_time) AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN value
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -value
                END)
            / 1e18 AS amount,
            SUM(value) / 1e18 AS volume,
            COUNT(*) AS txn_count
        FROM first_digital_usd_ethereum.FDUSD_evt_Transfer
        GROUP BY 1, 2
    ),
    pyusd AS (
        SELECT
            'PYUSD' as asset,
            DATE(evt_block_time) AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN value
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -value
                END)
            / 1e6 AS amount,
            SUM(value) / 1e6 AS volume,
            COUNT(*) AS txn_count
        FROM paypal_pyusd_ethereum.PYUSDImplementation_evt_Transfer
        GROUP BY 1, 2
    ),
    crvusd AS (
        SELECT
            'crvUSD' as asset,
            DATE(evt_block_time) AS dt,
            SUM(
                CASE
                    WHEN "sender" = 0x0000000000000000000000000000000000000000 THEN value
                    WHEN "receiver" = 0x0000000000000000000000000000000000000000 THEN -value
                END)
            / 1e18 AS amount,
            SUM(value) / 1e18 AS volume,
            COUNT(*) AS txn_count
        FROM curvefi_ethereum.crvUSD_Stablecoin_evt_Transfer
        GROUP BY 1, 2
    ),
    usde AS (
        SELECT
            'USDe' as asset,
            DATE(evt_block_time) AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN value
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -value
                END)
            / 1e18 AS amount,
            SUM(value) / 1e18 AS volume,
            COUNT(*) AS txn_count
        FROM ethena_labs_ethereum.USDe_evt_Transfer
        GROUP BY 1, 2
    ),
    buidl AS (
        SELECT
            'BUIDL' as asset,
            DATE(evt_block_time) AS dt,
            SUM(
                CASE
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN value
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -value
                    ELSE 0
                END)
            / 1e6 AS amount,
            SUM(value) / 1e6 AS volume,
            COUNT(*) AS txn_count
        FROM erc20_ethereum.evt_transfer
        WHERE contract_address = 0x7712c34205737192402172409a8F7ccef8aA2AEc -- BUIDL
        AND DATE(evt_block_time) > DATE '2024-02-29'
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
            SELECT * FROM usdp
            UNION ALL
            SELECT * FROM gusd
            UNION ALL
            SELECT * FROM busd
            UNION ALL
            SELECT * FROM tusd
            UNION ALL
            SELECT * FROM lusd
            UNION ALL
            SELECT * FROM fdusd
            UNION ALL
            SELECT * FROM pyusd
            UNION ALL
            SELECT * FROM crvusd
            UNION ALL
            SELECT * FROM usde
            UNION ALL
            SELECT * FROM sdai
            UNION ALL
            SELECT * FROM buidl
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