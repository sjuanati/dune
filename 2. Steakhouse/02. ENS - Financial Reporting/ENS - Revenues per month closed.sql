/*
-- @title: ENS - Revenues per month closed
-- @description: Calculates USD monthly revenues by Registrations, Renewals, and Short Name Claims over the past year.
-- @author: Steakhouse Financial
-- @notes: KPIs by current month, current closed month, previous month closed, previous month closed a year ago
-- @version:
    - 1.0 - 2024-03-13 - Initial version
*/

with
    entries as (
        select * from query_2244104 -- result_ens_accounting_main
    ),
    monthly_items as (
        select 
            'Registrations' as item,
            date_trunc('month', ts) as period,
            sum(case when cast(account as varchar) like '3211%' then amount end) as amount
        from entries
        group by 1,2
        union all
        select
            'Renewals' as item,
            date_trunc('month', ts) as period,
            sum(case when cast(account as varchar) like '3212%' then amount end) as amount
        from entries
        group by 1,2
        union all
        select
            'Short Name Claims' as item,
            date_trunc('month', ts) as period,
            sum(case when cast(account as varchar) like '3213%' then amount end) as amount
        from entries
        group by 1,2
    ),
    monthly_aggr as (
        select
            period,
            sum(amount) as amount
        from monthly_items
        group by 1
    ),
    monthly_aggr_prev as (
        select
            period,
            amount as amount_aggr,
            lag(amount, 1) over (order by period) as amount_aggr_prev_month,
            lag(amount, 12) over (order by period) as amount_aggr_prev_year
        from monthly_aggr
    ),
    monthly_totals as (
        select
            mi.item,
            mi.period,
            mi.amount,
            ma.amount_aggr / 1000000 as amount_aggr,
            ma.amount_aggr_prev_month  / 1000000 as amount_aggr_prev_month,
            ma.amount_aggr_prev_year  / 1000000 as amount_aggr_prev_year
        from monthly_items mi
        left join monthly_aggr_prev ma
            on mi.period = ma.period
    )

select *
from monthly_totals
where period >= date_trunc('month', current_date) - interval '13' month
  and period < date_trunc('month', current_date)
order by period desc
