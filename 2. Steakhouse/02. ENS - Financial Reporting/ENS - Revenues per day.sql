/*
-- @title: ENS - Revenues per day
-- @description: Calculates USD daily revenues by Registrations, Renewals, and Short Name Claims over the past year.
-- @author: Steakhouse Financial
-- @notes: N/A
-- @version:
    - 2.0 - 2024-02-19 - Added comment header, updated query formatting
    - 1.0 - 2023-10-02 - Initial version
*/

with entries as (
    select * from query_2244104 -- result_ens_accounting_main
),
daily_items as (
    select 
        '1' as rk,
        'Registrations' as item,
        date_trunc('day', ts) as period,
        sum(case when cast(account as varchar) like '3211%' then amount end) as amount
    from entries
    group by 3
    union all
    select
        '2' as rk,
        'Renewals' as item,
        date_trunc('day', ts) as period,
        sum(case when cast(account as varchar) like '3212%' then amount end) as amount
    from entries
    group by 3
    union all
    select
        '3' as rk,
        'Short Name Claims' as item,
        date_trunc('day', ts) as period,
        sum(case when cast(account as varchar) like '3213%' then amount end) as amount
    from entries
    group by 3
)

select 
    di.rk, 
    di.item, 
    di.period, 
    di.amount
from daily_items di
where di.period >= current_date - interval '365' day