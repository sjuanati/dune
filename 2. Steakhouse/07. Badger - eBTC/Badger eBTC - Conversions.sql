/*
-- @title: Badger eBTC - Conversions
-- @author: Steakhouse Financial
-- @description: provides indices and rates for calculating stETH amount & yield or USD prices on different tokens
        index: to convert from stETH token shares to token amounts
        factor: to calculate accumulated stETH yield based on exponential - logaritmic factor
        price_*: to calculate the USD price of WETH, stETH, WBTC and BADGER
-- @notes: N/A
-- @version:
        1.0 - 2024-09-04 - Initial version
        2.0 - 2024-09-24 - Added BTC price
*/

with
    -- tokens involved in Badger's eBTC dashboard
    tokens as (
        select contract_address from (
            values
                0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, -- WETH
                0xae7ab96520de3a18e5e111b5eaab095312d7fe84, -- STETH
                0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599, -- WBTC
                0x3472A5A71965499acd81997a54BBA8D852C6E53d  -- BADGER
            ) as t(contract_address)
    ),
    -- time series starting on 2024-03-15, when eBTC contracts were deployed
    periods as (
        select dt from unnest(sequence(date '2024-03-15', current_date, interval '1' day)) as t(dt)
    ),
    -- a) provides the daily rebasing index to convert stETH shares to amounts, because
    --    some operations (cdp open) do not emit the index by the CdpManager, while others do.
    -- b) if index hasn't been refreshed for the current day, we use yesterday's.
    steth_index as (
        select
            date(evt_block_time) as dt,
            (posttotalether / 1e18) / (posttotalshares / 1e18) as index
        from lido_ethereum.steth_evt_tokenrebased
        where date(evt_block_time) >= date '2024-03-15'
        union all
        select
            current_date as dt,
            (posttotalether / 1e18) / (posttotalshares / 1e18) as index
        from lido_ethereum.steth_evt_tokenrebased
        where date(evt_block_time) = current_date - interval '1' day
        and current_date not in (
            select date(evt_block_time)
            from lido_ethereum.steth_evt_tokenrebased
            where date(evt_block_time) = current_date
        )
    ),
    -- applies compound interest factor over stETH yield based on exp(sum(log(1 + apr/365)))
    cum_factors as (
       select
            dt,
            exp(sum(ln(daily_rate / coalesce(prev_daily_rate, daily_rate))) over (order by dt)) as factor
        from (
            select
                dt,
                index as daily_rate,
                lag(index) over (order by dt) as prev_daily_rate
            from steth_index
        )
    ),
    -- @TODO: use eBTC price instead of WBTC?
    prices as (
        select
            dt,
            sum(if(symbol = 'BTC', price, 0)) as price_btc,
            sum(if(symbol = 'WBTC', price, 0)) as price_wbtc,
            sum(if(symbol = 'WETH', price, 0)) as price_weth,
            sum(if(symbol = 'stETH', price, 0)) as price_steth,
            sum(if(symbol = 'BADGER', price, 0)) as price_badger
        from (
            -- ethereum: historical price for weth, steth, wbtc & badger
            select day as dt, symbol, price_close as price
            from prices.usd_daily
            join tokens using (contract_address)
            where blockchain = 'ethereum'
            and day between date '2024-03-15' and current_date - interval '1' day
            union all
            -- bitcoin: historical price for btc
            select day as dt, symbol, price_close as price
            from prices.usd_daily -- historical price
            where blockchain is null
            and symbol = 'BTC'
            and day between date '2024-03-15' and current_date - interval '1' day
            union all
            -- ethereum: current price for weth, steth, wbtc & badger
            select date(minute) as dt, symbol, price
            from prices.usd_latest
            join tokens using (contract_address)
            where blockchain = 'ethereum'
            union all
            -- bitcoin: current price for btc
            select date(minute) as dt, symbol, price
            from prices.usd_latest
            where blockchain is null
            and symbol = 'BTC'
        )
        group by 1
    )

select
    dt,
    i.index,
    f.factor,
    p.price_btc,
    p.price_wbtc,
    p.price_steth,
    p.price_weth,
    p.price_badger
from periods
left join steth_index i using(dt)
left join cum_factors f using(dt)
left join prices p using(dt)
order by dt desc