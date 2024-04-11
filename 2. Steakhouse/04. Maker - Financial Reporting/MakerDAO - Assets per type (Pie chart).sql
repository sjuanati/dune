-- deprecated
with lending_assets_1 as (
    select i as ilk, call_block_time, dart as dart, null as rate
    from makermcd."VAT_call_frob"
    where call_success
        and dart <> 0.0
    union all
    select i as ilk, call_block_time, dart as dart, 0.0 as rate
    from makermcd."VAT_call_grab"
    where call_success
        and dart <> 0.0
    union all
    select i as ilk, call_block_time, null as dart, rate as rate 
    from makermcd."VAT_call_fold"
    where call_success
        and rate <> 0.0
),
gusd_settings as (
    select period::date as dt, 0.0125 as rate, 100*10^6 min_volume
    from generate_series('2022-10-25'::date, current_date, '1 day') period
),
mip65_settings as (
    select period::date as dt, 
        case 
            when period >= '2022-10-27' then 0.042 
            when period >= '2022-10-13' then 0.03
        else 0.0 end as rate
    from generate_series('2022-10-13'::date, current_date, '1 day') period
),
-- Find the first usage of an ilk
ilks as (
    select ilk, min(call_block_time) as starting_use
    from lending_assets_1
    group by ilk
),
-- Generate one 'touch' per ilk per month to avoid holes
noop_filling as (
    select ilk, d as call_block_time, null::numeric as dart, null::numeric as rate, null::numeric as sf
    from ilks
    cross join generate_series(starting_use, current_date+1, '1 day') d
),
rates as (
    select call_block_time, ilk, (data/10^27)^(3600*24*365) -1 as sf
    from makermcd."JUG_call_file"
    where call_success 
),
lending_assets_1_with_filling as (
    select *, null::numeric as sf from lending_assets_1
    union all
    select * from noop_filling
    union all
    select ilk, call_block_time,  null::numeric as dart, null::numeric as rate, sf from rates
),
lending_assets_2 as (
    select ilk, call_block_time, rate as r,
        coalesce(1+sum(rate) over(partition by ilk order by call_block_time asc)/10^27,1) as rate,
        sum(dart) over(partition by ilk order by call_block_time asc)/10^18 as dart,
        sum(case when sf is not null then 1 else 0 end) over(partition by ilk order by call_block_time asc) as sf_grp,
        sf
    from lending_assets_1_with_filling 
),
with_rk as (
    select call_block_time::date as dt,
        replace(encode(ilk, 'escape'), '\000', '') as collateral, 
        dart*rate as debt,
        dart * (r/10^27) as revenues,
        max(sf) over(partition by ilk, sf_grp) as sf,
        row_number() over (partition by ilk, call_block_time::date order by call_block_time desc) as rk
    from lending_assets_2
),
group_by as (
    select *, sf as rate, debt*sf as annual_revenues, sum(revenues) over (partition by collateral, dt) as rev
    from with_rk
    where rk = 1
        and debt <> 0.0
),
d3m as (
    select dt, collateral, debt, coalesce(daily_revenue,0)*365  as annual_revenues, coalesce(daily_revenue,0) as rev
    from group_by
    left join  dune_user_generated.maker_aave_d3m_daily_revenues using (dt)
    where collateral = 'DIRECT-AAVEV2-DAI'
    union all
    select dt, collateral, debt, annual_revenues, rev
    from group_by
    where collateral <> 'DIRECT-AAVEV2-DAI'
),
group_by_cat as (
    select dt, 
        case when collateral like 'PSM%' then 'Stablecoins'
            when collateral in ('USDC-A','USDC-B', 'USDT-A', 'TUSD-A','GUSD-A','PAXUSD-A') then 'Stablecoins'
            when collateral like 'ETH-%' then 'ETH'
            when collateral like 'WSTETH-%' then 'ETH'
            when collateral like 'WBTC-%' then 'WBTC'
            when collateral like 'UNIV2%' then 'Liquidity Pools'
            when collateral like 'GUNI%' then 'Liquidity Pools'
            when collateral like 'RWA%' then 'RWA'
            when collateral like 'DIRECT%' then 'Money Markets'
            else 'Others' end as collateral,
            debt as asset,
            rev as revenues,
            case 
                when collateral = 'PSM-GUSD-A' and debt > gusd.min_volume then debt*gusd.rate 
                when collateral = 'RWA007-A'  then debt*mip65.rate 
                else annual_revenues 
            end as annual_revenues
    from d3m
    left join gusd_settings gusd using (dt)
    left join mip65_settings mip65 using (dt)
),
group_by_dt_cat as (
    select dt::timestamptz as dt,  collateral, sum(asset) as asset, sum(annual_revenues) as annual_revenues,  
        sum(annual_revenues)/sum(asset) as blended_rate, sum(revenues) as revenues
    from group_by_cat
    group by 1, 2
)
select *
    , sum(annual_revenues) over (partition by dt) as total_annual_revenues
    , sum(asset) over (partition by dt) as total_asset
    , sum(annual_revenues) over (partition by dt)/ sum(asset) over (partition by dt) as total_blended_rate
from group_by_dt_cat
where dt = current_date
order by 1 desc, 2