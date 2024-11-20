/*
-- @title: Burn & Buyback
-- @author: Steakhouse Financial
-- @description: Shows MKR/SKY buyback as the combination of MKR burned, MKR/SKY in treasury and MKR/SKY in Maker's Uniswap LP position
-- @version:
    - 1.0 - 2024-07-01 - Initial version
    - 2.0 - 2024-09-24 - Fixed MKR treasury amount, added SKY/USDS pool
    - 3.0 - 2024-10-27 - Fixed 0 value in lp_sky_amount & lp_usds_amount when no Sync events for the latest day
    - 4.0 - 2024-11-16 - Added SKY treasury amount & SKY buyback amount
    - 5.0 - 2024-11-17 - Added SKY-and-MKR metrics and updated chart to integrate MKR/SKY buyback process
*/

with
    periods as (
        select dt from unnest(sequence(date '2020-01-01', current_date, interval '1' day)) as d(dt)
    ),
    -- LP amounts in Uniswap V2 pools: DAI/MKR and SKY/USDS
    uni_lp_wallets as (
        select
            pool,
            date(evt_block_time) as dt,
            address,
            sum(value) / 1e18 as lp_amount
        from (
            select 'dai-mkr' as pool, evt_block_time, "to" as address, value from uniswap_v2_ethereum.Pair_evt_Transfer
            where contract_address = 0x517F9dD285e75b599234F7221227339478d0FcC8 -- Uniswap V2 DAI/MKR
            union all
            select 'dai-mkr' as pool, evt_block_time, "from" as address, -value from uniswap_v2_ethereum.Pair_evt_Transfer
            where contract_address = 0x517F9dD285e75b599234F7221227339478d0FcC8 -- Uniswap V2 DAI/MKR
            union all
            select 'sky-usds' as pool, evt_block_time, "to" as address, value from uniswap_v2_ethereum.Pair_evt_Transfer
            where contract_address = 0x2621CC0B3F3c079c1Db0E80794AA24976F0b9e3c -- Uniswap V2 SKY/USDS
            union all
            select 'sky-usds' as pool, evt_block_time, "from" as address, -value from uniswap_v2_ethereum.Pair_evt_Transfer
            where contract_address = 0x2621CC0B3F3c079c1Db0E80794AA24976F0b9e3c -- Uniswap V2 SKY/USDS
        )
        where address != 0x0000000000000000000000000000000000000000
        group by 1, 2, 3
    ),
    -- Date of first LP transfer per wallet
    uni_lp_mindates as (
        select pool, address, min(dt) as min_date from uni_lp_wallets group by 1, 2
    ),
    -- Date sequence for each wallet starting from the 1st LP transfer
    lp_period as (
        select
            seq.dt,
            md.pool,
            md.address,
            0 as lp_amount
        from uni_lp_mindates md
        cross join lateral (
            select dt from unnest(sequence(md.min_date, current_date, interval '1' day)) as t(dt)
        ) as seq
    ),
    -- Merging date sequence with LP amounts
    uni_lp_wallets_seq as (
        select
            dt,
            pool,
            address,
            coalesce(w.lp_amount, 0) as lp_amount
        from lp_period p
        left join uni_lp_wallets w using (dt, address, pool)
    ),
    -- Cumulative LP amounts per day, pool & wallet
    uni_lp_wallets_cum as (
        select
            w.dt,
            pool,
            address,
            sum(w.lp_amount) over (partition by pool, address order by w.dt) as lp_amount
        from uni_lp_wallets_seq w
        left join uni_lp_mindates md using(address, pool)
    ),
    -- Total LP amount per day & pool
    uni_lp_total as (
        select
            dt,
            pool,
            sum(lp_amount) as lp_amount
        from uni_lp_wallets_cum
        group by 1, 2
    ),
    -- DAI, MKR, SKY & USDS reserves in Uniswap pool per day (latest update per day)
    uni_reserves as (
        select
            dt,
            max(if(contract_address = 0x517F9dD285e75b599234F7221227339478d0FcC8, reserve0, null)) as dai_amount,
            max(if(contract_address = 0x517F9dD285e75b599234F7221227339478d0FcC8, reserve1, null)) as mkr_amount,
            max(if(contract_address = 0x2621CC0B3F3c079c1Db0E80794AA24976F0b9e3c, reserve0, null)) as sky_amount,
            max(if(contract_address = 0x2621CC0B3F3c079c1Db0E80794AA24976F0b9e3c, reserve1, null)) as usds_amount
        from (
            select
                date(evt_block_time) as dt,
                contract_address,
                reserve0 / 1e18 as reserve0,
                reserve1 / 1e18 as reserve1,
                rank() over (partition by contract_address, date(evt_block_time) order by evt_block_time desc) as rank
            from uniswap_v2_ethereum.Pair_evt_Sync
            where contract_address in (
                0x517F9dD285e75b599234F7221227339478d0FcC8, -- Uniswap V2 DAI/MKR
                0x2621CC0B3F3c079c1Db0E80794AA24976F0b9e3c  -- Uniswap V2 SKY/USDS
            )
        )
        where rank = 1
        group by dt
    ),
    -- DAI, MKR, SKY & USDS amounts from Maker's LP position in Uniswap pools
    lp_underlying_assets as (
        select
            dt,
            max(lp_amount) as lp_amount,
            max(lp_amount_dai_mkr) as lp_amount_dai_mkr,
            max(lp_amount_sky_usds) as lp_amount_sky_usds,
            max(dai_amount) as dai_amount,
            max(mkr_amount) as mkr_amount,
            max(sky_amount) as sky_amount,
            max(usds_amount) as usds_amount
        from (
            select
                p.dt,
                w.lp_amount,
                case when t.pool = 'dai-mkr' and w.pool = 'dai-mkr' then w.lp_amount else null end as lp_amount_dai_mkr,
                case when t.pool = 'sky-usds' and w.pool = 'sky-usds' then w.lp_amount else null end as lp_amount_sky_usds,
                case when t.pool = 'dai-mkr' and w.pool = 'dai-mkr' then (w.lp_amount / t.lp_amount) * r.dai_amount else null end as dai_amount,
                case when t.pool = 'dai-mkr' and w.pool = 'dai-mkr' then (w.lp_amount / t.lp_amount) * r.mkr_amount else null end as mkr_amount,
                case when t.pool = 'sky-usds' and w.pool = 'sky-usds' then (w.lp_amount / t.lp_amount) * r.sky_amount else null end as sky_amount,
                case when t.pool = 'sky-usds' and w.pool = 'sky-usds' then (w.lp_amount / t.lp_amount) * r.usds_amount else null end as usds_amount
            from periods p
            left join uni_lp_total t on p.dt = t.dt
            left join uni_reserves r on p.dt = r.dt
            left join uni_lp_wallets_cum w on p.dt = w.dt and w.address = 0xbe8e3e3618f7474f8cb1d074a26affef007e98fb -- Maker's DAO wallet
            )
        group by dt
    ),
    -- Cumulative DAI, MKR, SKY & USDS amounts from Maker's LP position in Uniswap per day
    lp_underlying_assets_cum as (
        select
            dt,
            coalesce(
                lp_amount_sky_usds,
                last_value(lp_amount_sky_usds) ignore nulls over (order by dt rows between unbounded preceding and current row)
            ) as lp_amount_sky_usds,
            coalesce(
                lp_amount_dai_mkr,
                last_value(lp_amount_dai_mkr) ignore nulls over (order by dt rows between unbounded preceding and current row)
            ) as lp_amount_dai_mkr,
            coalesce(
                dai_amount,
                last_value(dai_amount) ignore nulls over (order by dt rows between unbounded preceding and current row)
            ) as lp_dai_amount,
            coalesce(
                mkr_amount,
                last_value(mkr_amount) ignore nulls over (order by dt rows between unbounded preceding and current row)
            ) as lp_mkr_amount,
            coalesce(
                sky_amount,
                last_value(sky_amount) ignore nulls over (order by dt rows between unbounded preceding and current row)
            ) as lp_sky_amount,
            coalesce(
                usds_amount,
                last_value(usds_amount) ignore nulls over (order by dt rows between unbounded preceding and current row)
            ) as lp_usds_amount
        from lp_underlying_assets
    ),
    -- MKR amount held in DS Proxy
    mkr_dao_wallet as (
        select
            dt,
            sum(coalesce(w.amount, 0)) / 1e18 as mkr_amount
        from periods p
        left join (
            select
                date(evt_block_time) as dt,
                case
                    when "from" = 0xbe8e3e3618f7474f8cb1d074a26affef007e98fb then -value
                    when "to" = 0xbe8e3e3618f7474f8cb1d074a26affef007e98fb then value
                end as amount
            from maker_ethereum.mkr_evt_Transfer
            where 0xbe8e3e3618f7474f8cb1d074a26affef007e98fb in ("from", "to")
            and "from" != "to"
            union all
            select date(evt_block_time) as dt, wad * 1e0 as amount
            from maker_ethereum.mkr_evt_Mint
            where guy = 0xbe8e3e3618f7474f8cb1d074a26affef007e98fb
            union all
            select date(evt_block_time) as dt, -(wad * 1e0) as amount
            from maker_ethereum.mkr_evt_Burn
            where guy = 0xbe8e3e3618f7474f8cb1d074a26affef007e98fb
        ) w using (dt)
        group by 1
    ),
    -- Cumulative MKR amount held in DS Proxy
    mkr_dao_wallet_cum as (
        select
            dt,
            sum(coalesce(mkr_amount, 0)) over (order by dt asc) as treasury_mkr_amount
        from mkr_dao_wallet
    ),
    -- MKR amount burned before the implementation of Maker's Smart Burn Engine
    mkr_burned as (
        select
            dt,
            sum(b.burn_mkr_amount) as burn_mkr_amount
        from periods p
        left join (
            select
                date(k.evt_block_time) as dt,
                sum(coalesce(value / 1e18, 0)) as burn_mkr_amount
            from maker_ethereum.FLAP_evt_Kick k
            inner join maker_ethereum.FLAP_call_deal d
                using (contract_address, id)
            left join maker_ethereum.mkr_evt_Transfer t
                using (evt_tx_hash)
            where d.call_success = true
            and to in (
                0xdfe0fb1be2a52cdbf8fb962d5701d7fd0902db9f, -- Flap
                0xc4269cc7acdedc3794b221aa4d9205f564e27f0d  -- Flapper
            )
            group by 1
        ) b using (dt)
        group by 1
    ),
    -- Cumulative MKR amount burned
    mkr_burned_cum as (
        select
            dt,
            sum(coalesce(burn_mkr_amount, 0)) over (order by dt asc) as burn_mkr_amount
        from mkr_burned
    ),
    -- SKY amount held in DS Proxy
    sky_dao_wallet as (
        select
            dt,
            sum(coalesce(w.amount, 0)) / 1e18 as sky_amount
        from periods p
        left join (
            select
                date(evt_block_time) as dt,
                case
                    when "from" = 0xbe8e3e3618f7474f8cb1d074a26affef007e98fb then -value
                    when "to" = 0xbe8e3e3618f7474f8cb1d074a26affef007e98fb then value
                end as amount
            from sky_ethereum.SKY_evt_Transfer
            where 0xbe8e3e3618f7474f8cb1d074a26affef007e98fb in ("from", "to")
            and "from" != "to"
        ) w using (dt)
        group by 1
    ),
    -- Cumulative SKY amount held in DS Proxy
    sky_dao_wallet_cum as (
        select
            dt,
            sum(coalesce(sky_amount, 0)) over (order by dt asc) as treasury_sky_amount
        from sky_dao_wallet
    ),
    -- Final query grouping all metrics
    totals as (
        select
            dt,
            if(u.lp_amount_sky_usds < 1e-6, 0, u.lp_amount_sky_usds) as lp_amount_sky_usds,
            if(u.lp_amount_dai_mkr < 1e-6, 0, u.lp_amount_dai_mkr) as lp_amount_dai_mkr,
            if(u.lp_dai_amount < 1e-6, 0, u.lp_dai_amount) as lp_dai_amount,
            if(u.lp_mkr_amount < 1e-6, 0, u.lp_mkr_amount) as lp_mkr_amount,
            if(u.lp_sky_amount < 1e-6, 0, u.lp_sky_amount) as lp_sky_amount,
            if(u.lp_usds_amount < 1e-6, 0, u.lp_usds_amount) as lp_usds_amount,
            u.lp_sky_amount / 24000 as lp_sky_to_mkr_amount,
            coalesce(w.treasury_mkr_amount, 0) as treasury_mkr_amount,
            coalesce(s.treasury_sky_amount, 0) as treasury_sky_amount,
            coalesce(w.treasury_mkr_amount, 0) + coalesce(s.treasury_sky_amount / 24000, 0) as treasury_sky_and_mkr_amount,
            coalesce(b.burn_mkr_amount, 0) as burn_mkr_amount,
            coalesce(u.lp_mkr_amount, 0) + w.treasury_mkr_amount + b.burn_mkr_amount as buyback_mkr_amount,
            u.lp_sky_amount + s.treasury_sky_amount as buyback_sky_amount,
            coalesce(lp_mkr_amount, 0) + coalesce(lp_sky_amount / 24000, 0) as lp_mkr_and_sky_amount,
            coalesce(u.lp_mkr_amount, 0) + coalesce(w.treasury_mkr_amount, 0) + b.burn_mkr_amount
            + coalesce(u.lp_sky_amount / 24000, 0) + coalesce(s.treasury_sky_amount / 24000, 0) as buyback_mkr_and_sky_amount,
            1000000 - coalesce(u.lp_mkr_amount, 0) - coalesce(w.treasury_mkr_amount, 0) - b.burn_mkr_amount
            - coalesce(u.lp_sky_amount / 24000, 0) - coalesce(s.treasury_sky_amount / 24000, 0) as remaining_mkr_amount
        from lp_underlying_assets_cum u
        join mkr_dao_wallet_cum w using(dt)
        join sky_dao_wallet_cum s using(dt)
        join mkr_burned_cum b using(dt)
    )

select * from totals order by 1 desc
