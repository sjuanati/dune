with data as (
    select *
    from query_3286476
    where period = current_date - interval '1' day
),
lines as (
    select 1 as norder, '1. Day' as maturity, outflow_day as outflow, liquidity_day as liquidity, liquidity_day-outflow_day as funding_gap
    from data
    union all
    select 2 as norder, '2. Week' as maturity, outflow_week as outflow, liquidity_week as liquidity, liquidity_week-outflow_week as funding_gap
    from data
    union all
    select 3 as norder, '3. Month' as maturity, outflow_month as outflow, liquidity_month as liquidity, liquidity_month-outflow_month as funding_gap
    from data/*
    union all
    select 4 as norder, '4. Year' as maturity, outflow_year as outflow, liquidity_year as liquidity, liquidity_year-outflow_year as funding_gap*/
    -- Plug to fix some delta
    union all
    select 4 as norder, '4. Year' as maturity, 
        liquidity_day + liquidity_week + liquidity_month + liquidity_year - outflow_day - outflow_week - outflow_month as outflow, 
        liquidity_year as liquidity, 
        liquidity_year-( liquidity_day + liquidity_week + liquidity_month + liquidity_year - outflow_day - outflow_week - outflow_month) as funding_gap
    from data/*
    union all
    select 5 as norder, '5 - Total' as maturity, 
    outflow_day + outflow_week + outflow_month 
        + liquidity_day + liquidity_week + liquidity_month + liquidity_year - outflow_day - outflow_week - outflow_month  as outflow, 
        liquidity_day + liquidity_week + liquidity_month + liquidity_year as liquidity,0 as funding_gap
    from data*/
)
select norder, maturity, outflow, liquidity,
    sum(funding_gap) over (order by norder asc) as funding_gap
from lines
order by norder asc