/*
-- @title: Maker - DAI Maturity Profile - Monthly v2
-- @author: Steakhouse Financial
-- @description: Visualizes the cumulative monthly DAI balances categorized by maturity timeframes since 2020
-- @notes: N/A
-- @version:
    - 1.0 - 2024-06-18 - Initial version
*/

with
    deltas as (
        -- non-DSR DAI
        select
            dst as wallet,
            date(evt_block_time) as dt,
            wad as delta
        from maker_ethereum.DAI_evt_Transfer
        union all
        select
            src as wallet,
            date(evt_block_time) as dt,
            -wad as delta
        from maker_ethereum.DAI_evt_Transfer
        -- DSR DAI
        union all
        select
            0x197E90f9FAD81970bA7976f33CbD77088E5D7cf7 as wallet, -- pot
            date(ts) as dt,
            dai_value * 1e18 as delta
        from dune.steakhouse.result_maker_accounting_v2
        where code = 21110 -- Interest-bearing Dai
        -- Surplus Buffer
        union all
        select
            0xa950524441892a31ebddf91d3ceefa04bf454466 as wallet, -- vow
            date(ts) as dt,
            dai_value * 1e18 as delta
        from dune.steakhouse.result_maker_accounting_v2
        where cast(code as varchar) like '31%' -- Protocol Surplus
    ),
    maturities as (
        select
            'Speculative' as wallet,
            maturity,
            weight
        from (
            values
                ('1-block', 0.122000000000000000000),
                ('1-day', 0.122000000000000000000),
                ('1-week', 0.234000000000000000000),
                ('1-month', 0.061000000000000000000),
                ('3-months', 0.09000000000000000000),
                ('1-year', 0.371000000000000000000)
        ) as t(maturity, weight)
        union all
        select
            'Organic' as wallet,
            maturity,
            weight
        from (
            values
                ('1-block', 0.076000000000000000000),
                ('1-day', 0.076000000000000000000),
                ('1-week', 0.029000000000000000000),
                ('1-month', 0.00000000000000000000),
                ('3-months', 0.00000000000000000000),
                ('1-year', 0.819000000000000000000)
        ) as t(maturity, weight)    
        union all
        select
            'Surplus Buffer' as wallet,
            maturity,
            weight
        from (
            values
            ('1-block', 0.0),
            ('1-day', 0.0),
            ('1-week', 0.0),
            ('1-month', 0.0),
            ('3-months', 0.00),
            ('1-year', 1.0)
        ) as t(maturity, weight)
    ),
    contracts as (
        select distinct address from ethereum.contracts
    ),
    grouped as (
        select
            case
                when d.wallet = 0xa950524441892a31ebddf91d3ceefa04bf454466 then 'Surplus Buffer' -- vow
                when d.wallet = 0x197E90f9FAD81970bA7976f33CbD77088E5D7cf7 then 'Speculative'    -- pot
                when c.address is null then 'Organic'
                else 'Speculative'
            end as wallet,
            d.dt,
            sum(d.delta) as delta
        from deltas d
        left join contracts as c
            on d.wallet = c.address
        where d.wallet <> 0x0000000000000000000000000000000000000000
        group by 1, 2
    ),
    cum_balances as (
        select
            dt,
            wallet,
            sum(delta) over (partition by wallet order by dt) / 1e18 as balance
        from grouped
    ),
    sum_balances as (
        select
            dt,
            maturity,
            sum(balance * weight) as outflow,
            sum(case when wallet <> 'Surplus Buffer' then balance * weight end) as outflow_dai_only,
            sum(case when wallet = 'Surplus Buffer' then balance * weight end) as outflow_surplus_buffer
        from cum_balances
        join maturities using (wallet)
        group by 1, 2
    ),
    totals as (
        select
            dt,
            maturity,
            outflow,
            outflow_dai_only,
            outflow_surplus_buffer,
            sum(outflow) over (partition by dt) as total_period,
            max(dt) over (partition by year(dt), month(dt)) as last_day_of_month
        from sum_balances
    )

select *
from totals
where dt = last_day_of_month
and maturity != '1-block' -- 1-block is equivalent to 1-day
order by dt desc nulls first