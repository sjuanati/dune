/*
-- @title: Maker - DAI User Demographics
-- @author: Steakhouse Financial
-- @description: Displays the distribution of DAI holdings across different user account sizes over the latest 12-month period,
                 segmented by portfolio range from less than $100 to over $100 million
-- @notes: N/A
-- @version:
    - 1.0 - 2024-06-04 - Initial version
*/

with
    dai as (
        select
            date(evt_block_time) as dt,
            src as address,
            -wad as delta
        from maker_ethereum.DAI_evt_Transfer
        where src <> 0x0000000000000000000000000000000000000000
        union all
        select
            date(evt_block_time) as dt,
            dst as address,
            wad as delta
        from maker_ethereum.DAI_evt_Transfer
        where dst <> 0x0000000000000000000000000000000000000000
    ),
    period as (
        select dt from unnest(sequence(date('2019-11-01'), current_date, interval '1' day)) _u(dt)
    ),
    min_dates as (
        select address, min(dt) as min_dt from dai group by 1
    ),
    unioned as (
        select address, dt, delta
        from dai
        union all
        select address, dt, null
        from min_dates md
        join period p on p.dt > md.min_dt
    ),
    dai_sum as (
        select
            address,
            dt,
            sum(delta) as balance
        from unioned
        group by 1, 2
    ),
    dai_cum as (
        select
            dt,
            address,
            sum(balance) over (partition by address order by dt asc) / 1e18 as balance
        from dai_sum
    )

select
    dt,
    case
        when balance >= 100000000
        then '$100M+'
        when balance >= 10000000
        then '$10M-$100M'
        when balance >= 1000000
        then '$1M-$10M'
        when balance >= 100000
        then '$100K-$1M'
        when balance >= 10000
        then '$10K-$100K'
        when balance >= 1000
        then '$1K-$10K'
        when balance >= 100
        then '$100-$1K'
        when balance < 100
        then '<$100'
        when balance is null
        then 'null'
        else 'huh?'
    end as balance_size,
    sum(balance) as sum_balance
from dai_cum
where balance >= 0.01
and dt >= current_date - interval '12' month
group by 1, 2