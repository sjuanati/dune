/*
-- @title: ENS - Cashflows per month closed
-- @description: Calculates monthly cashflows for the last rolling year
-- @author: Steakhouse Financial
-- @notes: N/A
-- @version:
    - 1.0 - 2024-03-13 - Initial version
*/

with
    entries as (
        select * from query_2244104 -- ENS - Accounting - Main
    ),
    monthly_aggr as (
        select
            date_trunc('month', ts) as period,
            sum(case when cast(account as varchar) like '121%' then amount end) as amount
        from entries
        where ledger = 'CASH'
        group by 1
    ),
    monthly_aggr_prev as (
        select
            period,
            amount,
            lag(amount, 1) over (order by period) as amount_prev_month,
            lag(amount, 12) over (order by period) as amount_prev_year
        from monthly_aggr
    ),
    monthly_totals as (
        select
            period,
            amount,
            amount / 1000000 as amount_m,
            amount_prev_month  / 1000000 as amount_prev_month,
            amount_prev_year  / 1000000 as amount_prev_year
        from monthly_aggr_prev
    )

select *
from monthly_totals
where period >= date_trunc('month', current_date) - interval '13' month
  and period < date_trunc('month', current_date)
order by period desc