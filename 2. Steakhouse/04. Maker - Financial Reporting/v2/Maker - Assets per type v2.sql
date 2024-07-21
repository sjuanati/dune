-- revamp

with
    lending_assets as (
        select ilk, ts, call_trace_address, dart, rate
        from query_3754893 -- Maker - Interest Accruals v2
    ),
    gusd_settings as (
        select
            date(period) as dt,
            case
                when period < date'2023-07-01' then 0.0125
                else 0.028 -- https://forum.makerdao.com/t/gusd-makerdao-partnership-update-july-23/21401
            end as rate,
            1e8 as min_volume
        from unnest(sequence(date('2022-10-25'), current_date, interval '1' day)) as _u (period)
    ),
    mip65_settings as (
        select
            date(period) as dt,
            case
                when period >= date('2023-07-01') then 0.045
                when period >= date('2023-01-01') then 0.04
                when period >= date('2022-10-27') then 0.04
                when period >= date('2022-10-13') then 0.03
                else 0.0
            end as rate
        from unnest(sequence(date('2022-10-13'), current_date, interval '1' day)) as _u (period)
    ),
    -- Find the first usage of an ilk
    ilks as (
        select
            ilk,
            min(ts) as starting_use
        from lending_assets
        group by ilk
    ),
    -- Generate one 'touch' per ilk per month to avoid holes
    noop_filling  as (
        select
            ilk,
            cast(d as timestamp) - interval '1' second as ts, -- TODO: why not simplifying? date(d) as call_block_time,
            null as call_trace_address,
            null as dart,
            null as rate,
            null as sf
        from ilks
        cross join unnest (sequence(date(starting_use) + interval '1' day, current_date + interval '1' day, interval '1' day)) as _u (d)
    ),
    -- Rates for DIRECT-SPARK-DAI and DIRECT-SPARK-MORPHO-DAI not available in JUG
    rates as (
        select
            call_block_time as ts,
            ilk,
            power((data_uint256 / 1e27), (3600 * 24 * 365)) - 1 as sf
        from maker_ethereum.JUG_call_file
        where call_success
        and ilk not in (
            0x4449524543542d535041524b2d44414900000000000000000000000000000000, -- DIRECT-SPARK-DAI
            0x4449524543542d535041524b2d4d4f5250484f2d444149000000000000000000  -- DIRECT-SPARK-MORPHO-DAI
        )
        union ALL
        select
            period as ts,
            0x4449524543542d535041524b2d44414900000000000000000000000000000000 as ilk, -- DIRECT-SPARK-DAI
            supply_rate as rate
        from dune.steakhouse.result_lending_markets
        where blockchain = 'ethereum'
          and protocol = 'spark'
          and version = '1'
          and symbol = 'DAI'
        union all
        select
            period as ts,
            0x4449524543542d535041524b2d4d4f5250484f2d444149000000000000000000 as ilk, -- DIRECT-SPARK-MORPHO-DAI
            rate
        from query_3789881 -- MetaMorpho Markets Data
        where metamorpho = 0x73e65dbd630f90604062f6e02fab9138e713edd9 -- Spark DAI Vault
    ),
    lending_assets_with_filling as (
        select *, null as sf from lending_assets
        union ALL
        select * from noop_filling
        union all
        select
            ilk,
            ts,
            cast(null as array(bigint)) as call_trace_address,
            null as dart,
            null as rate,
            sf
        from rates
    ),
    lending_assets_cum as (
        select
            ilk,
            ts,
            rate as r,
            coalesce(1 + sum(rate) over (partition by ilk order by ts, call_trace_address) / 1e27, 1) as rate,
            sum(dart) over (partition by ilk order by ts, call_trace_address) / 1e18 as dart,
            sum(case when not sf is null then 1 else 0 end) over (partition by ilk order by ts) as sf_grp,
            sf
        from lending_assets_with_filling
    ),
    with_rk as (
        select
            date(ts) as dt,
            from_utf8(varbinary_rtrim(ilk)) as collateral,
            dart * rate as debt,
            dart * (r / 1e27) as revenues,
            max(sf) over (partition by ilk, sf_grp) as sf,
            row_number() over (partition by ilk, date(ts) order by ts desc) as rk
        from lending_assets_cum
    ),
    group_by as (
        select *
        from (
            select
                *,
                sf as rate,
                debt * sf as annual_revenues,
                sum(revenues) over (partition by collateral, dt) as rev
            from with_rk
        )
        where rk = 1
        and debt <> 0
    ),
    d3m as (
        select
            dt,
            collateral,
            debt,
            coalesce(daily_revenue, 0) * 365 as annual_revenues,
            coalesce(daily_revenue, 0) as rev
        from group_by
        left join (select * from query_3899343) sub USING (dt) -- a D3M daily revenue
        where collateral = 'DIRECT-AAVEV2-DAI'
        union ALL
        select
            dt,
            collateral,
            debt,
            coalesce(daily_revenue, 0) * 365 as annual_revenues,
            coalesce(daily_revenue, 0) as rev
        from group_by
        left join (select * from query_3898291) sub USING (dt) -- c D3M daily revenue
        where collateral = 'DIRECT-COMPV2-DAI'
        union ALL
        select
            dt,
            collateral,
            debt,
            annual_revenues,
            rev
        from group_by
        where collateral not IN ('DIRECT-AAVEV2-DAI', 'DIRECT-COMPV2-DAI')
        ),
    group_by_cat as (
        select
            dt,
            case
                when collateral like 'PSM%' then 'Stablecoins'
                when collateral IN ('USDC-A', 'USDC-B', 'USDT-A', 'TUSD-A', 'GUSD-A', 'PAXUSD-A') then 'Stablecoins'
                when collateral like 'ETH-%' then 'ETH'
                when collateral like 'WSTETH-%' then 'ETH'
                when collateral like 'WBTC-%' then 'WBTC'
                when collateral like 'UNIV2%' then 'Liquidity Pools'
                when collateral like 'GUNI%' then 'Liquidity Pools'
                when collateral like 'RWA015-A' then 'TBills'
                when collateral like 'RWA007-A' then 'TBills'
                when collateral like 'RWA014-A' then 'Coinbase'
                when collateral like 'RWA%' then 'RWA'
                when collateral like 'DIRECT%' then 'Lending Protocols'
                else 'Others'
            end as collateral_original,
            case
                when collateral like 'RWA%' then 'RWA'
                when collateral like 'PSM%' then 'PSM'
                when collateral like 'ETH-%' then 'ETH'
                when collateral like 'WSTETH-%' then 'STETH'
                when collateral like 'WBTC-%' then 'WBTC'
                else 'Other crypto'
            end as collateral,
            debt as asset,
            rev as revenues,
            case
                when collateral = 'PSM-GUSD-A' and debt > gusd.min_volume then debt * gusd.rate
                when collateral in ('RWA007-A', 'RWA009-A', 'RWA014-A', 'RWA015-A') then debt * mip65.rate
                else annual_revenues
            end as annual_revenues
        from d3m
        left join gusd_settings as gusd USING (dt)
        left join mip65_settings as mip65 USING (dt)
    ),
    group_by_dt_cat as (
        select
            dt,
            collateral,
            sum(asset) as asset,
            sum(annual_revenues) as annual_revenues,
            sum(annual_revenues) / sum(asset) as blended_rate,
            sum(revenues) as revenues
        from group_by_cat
        group by 1, 2
    )

select
    *,
    sum(annual_revenues) over (partition by dt) as total_annual_revenues,
    sum(asset) over (partition by dt) as total_asset,
    sum(annual_revenues) over (partition by dt) / sum(asset) over (partition by dt) as total_blended_rate
from group_by_dt_cat
where dt > date('2020-01-01')
order by 1 desc, 2