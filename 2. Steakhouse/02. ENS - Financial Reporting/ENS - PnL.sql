/*
-- @title: ENS - PnL
-- @description: Provides a monthly Profit and Loss over the past three years.
-- @author: Steakhouse Financial
-- @notes: N/A
-- @version:
    - 2.0 - 2024-02-19 - Added comment header
    - 1.0 - 2023-10-08 - Initial version
*/

with entries as (
    select cast(account as varchar) as account, amount, date_trunc('month', ts) as period from dune.steakhouse.result_ens_accounting_main
),
items as (
    select '329' as rk, 'Revenues' as item, period, sum(case when account like '321%' then amount end) as amount
    from entries
    group by period
    union all
    select '3211' as rk, 'Rev - Domain reg' as item, period, sum(case when account like '3211%' or account like '3213%' then amount end) as amount
    from entries
    group by period
    union all
    select '3212' as rk, 'Rev - Domain renew' as item, period, sum(case when account like '3212%' then amount end) as amount
    from entries
    group by period
    union all
    select '322' as rk, 'Op. Expenses' as item, period, sum(case when account like '322%' then amount end) as amount
    from entries
    group by period
    union all
    select '3231' as rk, 'Currencies effect' as item, period, sum(case when account like '3231%' then amount end) as amount
    from entries
    group by period
    union all
    select '3232' as rk, 'Investments P&L' as item, period, sum(case when account like '3232%' or account like '3233%' then amount end) as amount
    from entries
    group by period
    union all
    select '5' as rk, 'P&L (excl. FX)' as item, period, sum(case when account like '32%' and account not like '3231%' then amount end) as amount
    from entries
    group by period
)
select item, period, amount
from items
where period >= current_date - interval '3' year
    and period < date_trunc('month', current_date)
order by rk asc