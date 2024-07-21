-- revamp

with
    surplus as (
        select
            date(ts) as period,
            sum(case when cast(code as varchar) like '31%' then dai_value end)
            as protocol_surplus
        from dune.steakhouse.result_maker_accounting_v2
        where cast(code as varchar) like '3%'
        group by 1
    ),
    surplus_agg as (
        select
            period,
            sum(protocol_surplus) over (order by period) as surplus_buffer
        from surplus
    ),
    surplus_agg_lag as (
        select
            period,
            surplus_buffer,
            (surplus_buffer - LAG(surplus_buffer, 30) over (order by period)) / 30 * 365 as delta_30d,
            (surplus_buffer - LAG(surplus_buffer, 90) over (order by period)) / 90 * 365 as delta_90d
        from surplus_agg
    )

select * from surplus_agg_lag order by 1 desc