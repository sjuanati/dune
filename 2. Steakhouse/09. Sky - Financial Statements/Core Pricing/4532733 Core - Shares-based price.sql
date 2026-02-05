/*
-- @title: Core - Shares-based price
-- @description: Handles prices derived from shares price event tokens
-- @author: Steakhouse Financial
-- @notes: when adding new prices, remember updating matview query_4583090
-- @version:
    - 1.0 - 2025-01-07 - Initial version
    - 2.0 - 2025-03-05 - Add in srUSD pricing from rUSD swapExecuted events
    - 3.0 - 2025-04-24 - Set shares > N (and not 0) to avoid wrong pricing in very smol shares amount
                       - Moved sUSDS calc to the Oracle-based price query
                       - Moved srUSD calc to the Others' price query (using the PSM)
                       - Removed union with default & dex prices and using a coalesce to get the price by order of priority
    - 4.0 - 2025-04-29 - Added JTRSY pricing based on query_4045326 & query_4045054
    - 5.0 - 2025-05-05 - Fixed stablecoin-based tokens precision by using only share price and assuming $1 as underlying token
                         to avoid dune's spiky calculations on stablecoins (using field <use_share_price>)
    - 6.0 - 2025-05-12 - Added price feed from graphql via http requests for jtrsy
    - 7.0 - 2025-05-13 - Morpho part moved out as some vault tokens are also calculated within this query (eg: wUSDL)
    - 8.0 - 2025-05-14 - Added stUSD pricing
    - 9.0 - 2025-05-25 - Added sUSDE pricing + replaced prices.usd_daily & dex.prices by prices.day
    - 10.0 - 2025-06-03 - Added TBILL pricing (from OpenEden)
    - 11.0 - 2025-07-24 - Update oracle api for JTRSY
    - 12.0 - 2025-07-31 - Add JAAA oracle
    - 13.0 - 2025-08-17 - Remove Midas (it appears in Oracle Prices)
    - 14.0 - 2025-10-07 - Add syrup
                        -> Add pricing for sUSDS from our Alternative Matview
    - 15.0 - 2026-01-08 - Add ACRDX from Centrifuge
*/

with
    ---------------------------------------------
    -- Ethena's sUSDE
    ---------------------------------------------
    susde as (
        select
            date(dt) as dt,
            'ethereum' as blockchain,
            'sUSDe' as symbol,
            0x9D39A5DE30e57443BfF2A8307A4256c8797A3497 as token_address, -- sUSDe
            0x4c9EDD5852cd905f086C759E8383e09bff1E68B3 as price_address, -- USDe
            true as use_share_price,
            max_by(assets, dt) as assets,
            max_by(shares, dt) as shares
        from (
            select evt_block_time as dt, assets, shares from ethena_labs_ethereum.stakedusdev2_evt_deposit where shares > 1e10
            union all
            select evt_block_time as dt, assets, shares from ethena_labs_ethereum.stakedusdev2_evt_withdraw where shares > 1e10
        )
        group by 1
    ),
    ---------------------------------------------
    -- stUSD
    ---------------------------------------------
    stusd_tokens (blockchain, token_address, price_address) as (
        values
            ('base', 0x0022228a2cc5E7eF0274A7Baa600d44da5aB5776, 0x0000206329b97db379d5e1bf586bbdb969c63274),
            ('ethereum', 0x0022228a2cc5E7eF0274A7Baa600d44da5aB5776, 0x0000206329b97db379d5e1bf586bbdb969c63274)
    ),
    stusd as (
        select
            date(tr.dt) as dt,
            tk.blockchain as blockchain,
            'stUSD' as symbol,
            tk.token_address, -- stUSD
            tk.price_address, -- USDA
            true as use_share_price,
            max_by(tr.assets, dt) as assets,
            max_by(tr.shares, dt) as shares
        from (
            select evt_block_time as dt, assets, shares from angle_ethereum.stusd_evt_deposit where shares > 1e10
            union all
            select evt_block_time as dt, assets, shares from angle_ethereum.stusd_evt_withdraw where shares > 1e10
        ) tr
        cross join stusd_tokens tk
        group by 1,2,4,5
    ),
    ---------------------------------------------
    -- wUSDL
    ---------------------------------------------
    wusdl as (
        select
            date(dt) as dt,
            'ethereum' as blockchain,
            'wUSDL' as symbol,
            0x7751E2F4b8ae93EF6B79d86419d42FE3295A4559 as token_address, -- wUSDL
            0xbdc7c08592ee4aa51d06c27ee23d5087d65adbcd as price_address, -- USDL
            true as use_share_price,
            max_by(assets, dt) as assets,
            max_by(shares, dt) as shares
        from (
            select evt_block_time as dt, assets, shares from wusdl_ethereum.wybsv1_evt_withdraw where shares > 1e10
            union all
            select evt_block_time as dt, assets, shares from wusdl_ethereum.wybsv1_evt_deposit where shares > 1e10
        )
        group by 1
    ),
    ---------------------------------------------
    -- Dinero's ApxETH
    ---------------------------------------------
    apxeth as (
        select
            date(dt) as dt,
            'ethereum' as blockchain,
            'apxETH' as symbol,
            0x9Ba021B0a9b958B5E75cE9f6dff97C7eE52cb3E6 as token_address, -- apxETH
            0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as price_address, -- WETH
            false as use_share_price,
            max_by(assets, dt) as assets,
            max_by(shares, dt) as shares
        from (
            select evt_block_time as dt, assets, shares from pirex_ethereum.AutoPxEth_evt_Deposit where shares > 1e10
            union all
            select evt_block_time as dt, assets, shares from pirex_ethereum.AutoPxEth_evt_Withdraw where shares > 1e10
        )
        group by 1
    ),
    ---------------------------------------------
    -- Anzen's sUSDz
    ---------------------------------------------
    susdz as (
        select
            date(dt) as dt,
            'ethereum' as blockchain,
            'sUSDz' as symbol,
            0x547213367cfb08ab418e7b54d7883b2c2aa27fd7 as token_address, -- sUSDz
            0xA469B7Ee9ee773642b3e93E842e5D9b5BaA10067 as price_address, -- USDZ
            true as use_share_price,
            max_by(assets, dt) as assets,
            max_by(shares, dt) as shares
        from (
            select evt_block_time as dt, assets, shares from anzen_finance_v2_ethereum.SUSDz_evt_Deposit where shares > 1e10
            union all
            select evt_block_time as dt, assets, shares from anzen_finance_v2_ethereum.SUSDz_evt_Withdraw where shares > 1e10
        )
        group by 1
    ),
    ---------------------------------------------
    -- Tokemak's autoETH, autoLRT, balETH
    ---------------------------------------------
    tokemak_info (contract_address, symbol, decimals) as (
        values
            (0x0a2b94f6871c1d7a32fe58e1ab5e6dea2f114e56, 'autoETH', 18),
            (0xe800e3760fc20aa98c5df6a9816147f190455af3, 'autoLRT', 18),
            (0x6dc3ce9c57b20131347fdc9089d740daf6eb34c5, 'balETH', 18)
    ),
    tokemaks as (
        select
            date(n.evt_block_time) as dt,
            'ethereum' as blockchain,
            i.symbol as symbol,
            contract_address as token_address,
            0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as price_address,   -- WETH
            false as use_share_price,
            max_by(idle + debt, evt_block_time) as assets,
            max_by(totalSupply, evt_block_time) as shares
        from tokemak_ethereum.AutopoolETH_evt_Nav n
        join tokemak_info i using (contract_address)
        where totalSupply > 1e10
        group by 1, 2, 3, 4
    ),
    ---------------------------------------------
    -- Berachain's beraSTONE
    ---------------------------------------------
    stakestone as (
        select
            date(dt) as dt,
            'ethereum' as blockchain,
            'beraSTONE' as symbol,
            0x97Ad75064b20fb2B2447feD4fa953bF7F007a706 as token_address, -- beraSTONE
            0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as price_address, -- WETH
            false as use_share_price,
            max_by(amount, dt) as assets,
            max_by(shares, dt) as shares
        from (
            select evt_block_time as dt, asset, amount, shares
            from stakestone_v2_ethereum.StoneBeraVault_evt_Deposit
            where shares > 1e10
        )
        group by 1, 5
    ),
    ------------------------------------------------
    -- S Y R U P + + + + F L U I D
    ------------------------------------------------
    tokens(start_ts, contract_address, asset_address, symbol, decimals) as (
        VALUES
        (TIMESTAMP'2024-06-15 18:31', 0x80ac24aA929eaF5013f6436cdA2a7ba190f5Cc0b, 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48, 'syrupUSDC', 6),
        -- (TIMESTAMP'2023-12-17 02:22', 0x6174a27160f4d7885db4ffed1c0b5fbd66c87f3a, 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48, 'MPLohyUSDC1', 6),
        (TIMESTAMP'2024-08-09 16:47', 0x356b8d89c1e1239cbbb9de4815c39a1474d5ba7d, 0xdac17f958d2ee523a2206206994597c13d831ec7, 'syrupUSDT', 6),
        (TIMESTAMP'2025-01-16 22:59', 0xf62e339f21d8018940f188F6987Bcdf02A849619, 0x5875eEE11Cf8398102FdAd704C9E96607675467a, 'fsUSDS', 18),
        (TIMESTAMP'2025-01-14 06:04', 0x2bbe31d63e6813e3ac858c04dae43fb2a72b0d11, 0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD, 'fsUSDS', 18)
    ),
    syrup_events as (
        select ts as last_ts, symbol
            , contract_address
            , asset_address
            , blockchain
            , price as last_price
            , LAG(ts) IGNORE NULLS OVER (PARTITION BY contract_address ORDER BY ts) as last_last_ts
            , LAG(price) IGNORE NULLS OVER (PARTITION BY contract_address ORDER BY ts) as last_last_price
            , COALESCE(lead(ts) IGNORE NULLS OVER (PARTITION BY contract_address ORDER BY ts), NOW()) as next_ts
            , LEAD(price) IGNORE NULLS OVER (PARTITION BY contract_address ORDER BY ts) as next_price
            , LEAD(ts, 2) IGNORE NULLS OVER (PARTITION BY contract_address ORDER BY TS) as next_next_ts
            , LEAD(price, 2) IGNORE NULLS OVER (PARTITION BY contract_address ORDER BY TS) as next_next_price
        FROM (
            SELECT evt_block_time as ts, 'ethereum' as blockchain, contract_address, asset_address, symbol, ((assets_/POWER(10, decimals))/ (shares_/POWER(10, decimals))) as price
            FROM maplefinance_v2_ethereum.pool_v2_evt_deposit join tokens using(contract_address)
            where shares_ != 0
            UNION ALL
            SELECT evt_block_time as ts, 'ethereum' as blockchain, contract_address, asset_address, symbol, ((assets_/POWER(10, decimals))/ (shares_/POWER(10, decimals))) as price
            FROM maplefinance_v2_ethereum.pool_v2_evt_deposit join tokens using(contract_address)
            where shares_ != 0
            UNION ALL
            SELECT evt_block_time as ts, chain as blockchain, contract_address, asset_address, symbol, ((assets/POWER(10, decimals))/ (shares/POWER(10, decimals))) as price
            FROM fluid_multichain.ftoken_evt_deposit join tokens using(contract_address)
            where shares >= 1e2
            UNION ALL
            SELECT evt_block_time as ts, chain as blockchain, contract_address, asset_address, symbol, ((assets/POWER(10, decimals))/ (shares/POWER(10, decimals))) as price
            FROM fluid_multichain.ftoken_evt_withdraw join tokens using(contract_address)
            where shares >= 1e2
            
        ) as mpl
    ),
    syrup_series as (
        select dt, symbol, contract_address
        from tokens, unnest(sequence(DATE(tokens.start_ts), current_date, interval '1' day)) as s(dt)
    ),
    syrup_pricing as (
        SELECT s.dt
            , blockchain
            , s.symbol
            , s.contract_address
            , se.asset_address
            , CASE
                WHEN last_price is not null and next_price is not null
                    THEN last_price + (next_price - last_price)/date_diff('second', last_ts, next_ts) * (date_diff('second', last_ts, dt))
                WHEN next_price is NOT null and next_next_price is not null
                    THEN next_price - (next_next_price - next_price)/date_diff('second', next_next_ts, next_ts) * (date_diff('second', dt, next_ts))
                WHEN last_price is NOT null and last_last_price is not null and last_last_ts != last_ts
                    THEN last_price + ((last_price - last_last_price) / date_diff('second', last_last_ts, last_ts)) * (date_diff('second', last_ts, dt))
                END as price
        FROM syrup_series as s
            JOIN syrup_events as se on s.contract_address = se.contract_address and s.dt between se.last_ts and se.next_ts 
    ),
    syrup as (
        SELECT dt, blockchain, symbol, contract_address, asset_address
            , CASE WHEN symbol = 'fsUSDS' then false else true end as use_share_price
            , price as assets
            , 1 as shares
        FROM syrup_pricing
    ),
    ------------------------------------------------
    -- Janus Henderson Anemony Treasury Fund (JTRSY)
    ------------------------------------------------
    jh_info (blockchain, contract_address, pool_id, symbol) as (
        values
            ('celo', 0x27e8C820d05aEa8824b1aC35116f63f9833b54C8, 4139607887, 'JTRSY'),
            ('base', 0x8c213ee79581Ff4984583C6a801e5263418C4b86, 4139607887, 'JTRSY'),
            ('ethereum', 0x8c213ee79581Ff4984583C6a801e5263418C4b86, 4139607887, 'JTRSY'),
            ('arbitrum', 0x6d2b49608a716e30bc7abcfe00181bf261bf6fc5, 4139607887, 'JTRSY'),
            ('plume', 0xa5d465251fBCc907f5Dd6bB2145488DFC6a2627b, 4139607887, 'JTRSY'),
            ('ethereum', 0x5a0f93d040de44e78f251b03c43be9cf317dcf64, 158696445, 'JAAA'),
            ('avalanche_c', 0x58F93d6b1EF2F44eC379Cb975657C132CBeD3B6b, 158696445, 'JAAA'),
            ('plume', 0x9477724Bb54AD5417de8Baff29e59DF3fB4DA74f, 281474976710664, 'ACRDX')
    ),
    jh_pools(symbol, pool_type, pool_id, token_id, start_ts, start_ts_tz) as (
        values
            ('JTRSY', 'RWA', 4139607887, '0x00010000000000060000000000000001', TIMESTAMP'2025-04-19 00:00:00', '2025-04-19T00:00:00.000Z') -- JTRSY
            , ('JAAA', 'RWA', 158696445, '0x00010000000000070000000000000001', TIMESTAMP'2025-07-23 00:32:24', '2025-07-23T00:32:24.000Z') -- JAAA
            , ('ACRDX', 'RWA', 281474976710664, '0x00010000000000080000000000000001', TIMESTAMP'2025-09-19 00:41:37', '2025-09-19T00:41:37.000Z')
    ),
    jh_dune as (
        -- select
        --     date(ts) as dt,
        --     i.blockchain,
        --     i.symbol,
        --     i.contract_address as token_address,
        --     null as price_address,
        --     true as use_share_price,
        --     max_by(try_cast(json_value(t.pv, 'strict $.nav.total') as uint256) * 1e-6, ts) as assets,
        --     max_by(try_cast(json_query(t.pv, 'strict $.tranches[0].share') as uint256) * 1e-6, ts) as shares
        -- from centrifuge.traces t
        -- cross join jh_info i
        -- where t.track = 'pool'
        --   and try_cast(json_value(t.kv, 'strict $') as bigint) = 4139607887 -- filtering for Janus Henderson only
        --   and try_cast(json_value(t.pv, 'strict $.tranches[0].share') as uint256) > 0
        --   and date(ts) < date '2025-04-19' and date(ts) !=date'2024-05-15'
        -- group by 1,2,3,4
        -- UNION ALL
        -- SELECT timestamp'2024-05-15 00:00' as dt, blockchain, symbol, contract_address as token_address
        --     , null as price_address
        --     , true as use_share_price
        --     , 1.0180118128436235  as assets
        --     , 1 as shares
        -- from jh_info
        SELECT from_iso8601_timestamp(dt) as dt, blockchain, symbol, token_address, TRY_CAST(null as VARBINARY) as price_address
            , true as use_share_price
            , price_usd as assets
            , 1 as shares
        FROM dune.steakhouse.dataset_janus_dataset
    )
    , jtrsy_http_raw as (
        select json_extract(
            json_parse(
                TRY_CAST(try(
                    http_post(
                        'https://api.centrifuge.io/',
                        format('{"query":"query ($filter: TokenInstanceSnapshotFilter) {\n tokenInstanceSnapshots(\n where: $filter\n orderBy: \"timestamp\"\n orderDirection: \"asc\"\n limit: 1000\n) {\n items {\n tokenId\n timestamp\n tokenPrice\n}\n}\n}","variables":{"filter":{"tokenId_in":["%s"],"trigger_ends_with":"NewPeriod"}}}', token_id),
                        array['Content-Type: application/json']
                    )
                ) AS VARCHAR)), '$.data.tokenInstanceSnapshots.items'
        ) as node_array
        , pool_id, start_ts, symbol
        FROM jh_pools
    )
    , jh_http_formatted as (
        select
            date(from_unixtime(CAST(json_extract_scalar(node, '$.timestamp') AS double)/1000)) as dt,
            pool_id, start_ts,
            MAX_BY(cast(json_extract_scalar(node, '$.tokenPrice') as double) / 1e18, from_unixtime(CAST(json_extract_scalar(node, '$.timestamp') AS double)/1000)) AS price
        from jtrsy_http_raw
        cross join unnest(cast(node_array as array(JSON))) as t(node)
        GROUP BY 1, 2, 3
    
    ),
    jh_http as (
        select
            f.dt,
            i.blockchain,
            i.symbol,
            i.contract_address as token_address,
            null as price_address,
            true as use_share_price,
            -- Error in Price for the API
            -- CASE 
                -- WHEN f.dt = timestamp'2025-07-24 00:00' and i.symbol = 'JTRSY' THEN 1.0720150300002596
                -- WHEN f.dt = TIMESTAMP'2025-05-28 00:00' and i.symbol = 'JTRSY' THEN 1.065308596873840145 
                -- ELSE 
                f.price
            -- END 
            as assets,
            1 as shares
        from jh_http_formatted f
        join unnest(sequence(date(start_ts), current_date, interval '1' day)) as s(dt) on true
        left join jh_info i using(pool_id)
        where f.dt = s.dt and s.dt > date'2025-08-27' 
    ),
    ---------------------------------------------
    -- OpenEden's TBILL
    ---------------------------------------------
    tbill as (
        select
            date(dt) as dt,
            'ethereum' as blockchain,
            'TBILL' as symbol,
            0xdd50c053c096cb04a3e3362e2b622529ec5f2e8a as token_address, -- TBILL
            0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 as price_address, -- USDC
            true as use_share_price,
            max_by(assets, dt) as assets,
            max_by(shares, dt) as shares
        from (
            select evt_block_time as dt, assets, shares from tbill_ethereum.openedenvaultv2_evt_deposit where shares > 1e10
            union all
            select evt_block_time as dt, assets, shares from tbill_ethereum.openedenvaultv2_evt_withdraw where shares > 1e10
        )
        group by 1
    ),
    ---------------------------------------------
    -- Yearn V3 Vaults
    ---------------------------------------------
    yearn as (
        select
            date(dt) as dt,
            'katana' as blockchain,
            'yvvbUSDC' as symbol,
            0x80c34BD3A3569E126e7055831036aa7b212cB159 as token_address, -- yvvbUSDC
            0x203A662b0BD271A6ed5a60EdFbd04bFce608FD36 as price_address, -- vbUSDC
            true as use_share_price,
            max_by(assets, dt) as assets,
            max_by(shares, dt) as shares
        from (
            select evt_block_time as dt, assets, shares from katana_katana.yearn_v3_vault_usdc_evt_deposit where shares > 1e10
            union all
            select evt_block_time as dt, assets, shares from katana_katana.yearn_v3_vault_usdc_evt_withdraw where shares > 1e10
        )
        group by 1
    ),
    ------------------------------------------------
    -- all together
    ------------------------------------------------
    assets as (
        select * from susde
        union all
        select * from stusd
        union all
        select * from wusdl
        union all
        select * from apxeth
        union all
        select * from susdz
        union all
        select * from tokemaks
        union all
        select * from stakestone
        union all
        select * from jh_dune
        union all
        select * from jh_http
        union all
        select * from tbill
        union all
        select * from yearn
        union all
        select * from syrup
    ),
    default_pricing as (
        select
            date(p."timestamp") as dt,
            a.blockchain,
            a.symbol,
            a.token_address,
            a.price_address,
            assets / cast(shares as double) as share_price,
            min_by(p.price, source) * (assets / cast(shares as double)) as price_usd
        from (
            SELECT timestamp, source, blockchain, contract_address, price
            FROM prices.day
            where contract_address not in (
                0x5875eEE11Cf8398102FdAd704C9E96607675467a, -- sUSDS (base)
                0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD, -- sUSDS (ethereum)
                0xdDb46999F8891663a8F2828d25298f70416d7610, -- sUSDS (arbitrum)
                0xa06b10db9f390990364a3984c04fadf1c13691b5, -- sUSDS (unichain)
                0xb5b2dc7fd34c249f4be7fb1fcea07950784229e0 -- sUSDS (optimism)
            )
            UNION ALL
            SELECT dt, 'matview' as source, blockchain, token_address, price_usd
            FROM dune.steakhouse.result_token_alternative_price
            where token_address in (
                0x5875eEE11Cf8398102FdAd704C9E96607675467a, -- sUSDS (base)
                0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD, -- sUSDS (ethereum)
                0xdDb46999F8891663a8F2828d25298f70416d7610, -- sUSDS (arbitrum)
                0xa06b10db9f390990364a3984c04fadf1c13691b5, -- sUSDS (unichain)
                0xb5b2dc7fd34c249f4be7fb1fcea07950784229e0 -- sUSDS (optimism)
            )
        ) p
        join assets a
            on date(p."timestamp") = a.dt
            and p.blockchain = a.blockchain
            and p.contract_address = a.price_address
        group by 1,2,3,4,5,6
    ),
    -- @dev: for RWA without an underlying token to get the USD value, or stablecoin-based tokens for which we want
    -- to use the share price and assume the underlying token price is 1$; otherwise, the dune prices from stablecoins
    -- will add spikes to the share price that should be always constant and increasing
    share_pricing as (
        select
            dt,
            blockchain,
            symbol,
            token_address,
            price_address,
            assets / cast(shares as double) as share_price,
            assets / cast(shares as double) as price_usd
        from assets
        where shares > 0
          and use_share_price
    ),
    pricing as (
        select
            dt,
            blockchain,
            a.symbol,
            token_address,
            a.price_address, 
            df.price_usd as default_price_usd,
            coalesce(sh.share_price, df.share_price) as share_price,
            if(a.use_share_price, sh.price_usd, df.price_usd) as price_usd
        from assets a
        left join default_pricing df using (dt, blockchain, token_address)
        left join share_pricing sh using (dt, blockchain, token_address)
    )
select * from pricing