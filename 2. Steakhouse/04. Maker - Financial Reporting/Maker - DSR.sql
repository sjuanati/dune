with pots_events as (
    select call_block_time as ts, null as inflow, cast(rad as double)/pow(10,45) as outflow, null as interest
    from maker_ethereum.vat_call_move
    where call_success
        and src = 0x197e90f9fad81970ba7976f33cbd77088e5d7cf7
    union all
    select call_block_time as ts, cast(rad as double)/pow(10,45) as inflow, null as outflow,  null as interest
    from maker_ethereum.vat_call_move
    where call_success
        and dst = 0x197e90f9fad81970ba7976f33cbd77088e5d7cf7
    union all
    select call_block_time as ts, null as inflow, null as outflow, cast(rad as double)/pow(10,45) as interest
    from maker_ethereum.vat_call_suck
    where call_success
        and u = 0xa950524441892a31ebddf91d3ceefa04bf454466 -- Vow
        and v = 0x197e90f9fad81970ba7976f33cbd77088e5d7cf7 -- Pot
    union all
    select ts, null as inflow, null as outflow, null as interest
    from unnest(sequence(date('2020-12-01'), current_date - interval '1' day, interval '1' day)) as t(ts)
),
grp as (
    select cast(ts as date) as period, sum(inflow) as inflow, sum(outflow) as outflow, sum(interest) as interest
    from pots_events
    group by 1
),
windows as (
    select cast(period as timestamp) as period, 
        case when inflow > outflow then inflow - outflow end as inflow, 
        case when outflow > inflow then outflow - inflow end as outflow, 
        interest, 
        sum(coalesce(inflow, 0) - coalesce(outflow, 0) + coalesce(interest, 0)) over (order by period) as balance
    from grp
)
select *
from windows
where period >= current_date - interval '580' day
order by 1 desc