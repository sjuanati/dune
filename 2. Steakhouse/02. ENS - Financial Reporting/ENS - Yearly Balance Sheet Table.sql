/*
-- @title: ENS - Yearly Balance Sheet Table
-- @description: Generates a yearly balance sheet table for a comprehensive view of ENS's financial position from 2019.
-- @author: Steakhouse Financial
-- @notes: N/A
-- @version:
    - 2.0 - 2024-02-19 - Added comment header, updated final query to retrieve data for year 2024
    - 1.0 - 2023-10-02 - Initial version
*/

with entries as (
    select * from dune.steakhouse.result_ens_accounting_main
),
items as (
    select '1' as rk, '<b>Assets</b>' as item, year(ts) as period, sum(case when cast(account as varchar) like '1%' then amount end) as amount
    from entries
    group by year(ts)
    union all
    select '111' as rk, '&nbsp;&nbsp;&nbsp;&nbsp;<i>Cash</i>' as item, year(ts) as period, sum(case when cast(account as varchar) like '111%' then amount end) as amount
    from entries
    group by year(ts)
    union all
    select '112' as rk, '&nbsp;&nbsp;&nbsp;&nbsp;<i>Money Markets</i>' as item, year(ts) as period, sum(case when cast(account as varchar) like '112%' then amount end) as amount
    from entries
    group by year(ts)
    union all
    select '110' as rk, '&nbsp;&nbsp;Cash & cash equivalents' as item, year(ts) as period, sum(case when cast(account as varchar) like '11%' then amount end) as amount
    from entries
    group by year(ts)
    union all
    select '12' as rk, '&nbsp;&nbsp;ETH' as item, year(ts) as period, sum(case when cast(account as varchar) like '12%' then amount end) as amount
    from entries
    group by year(ts)
    union all
    select '13' as rk, '&nbsp;&nbsp;ETH Investments' as item, year(ts) as period, sum(case when cast(account as varchar) like '13%' then amount end) as amount
    from entries
    group by year(ts)
    union all
    select '2' as rk, '<b>Liabilities</b>' as item, year(ts) as period, sum(case when cast(account as varchar) like '2%' then amount end) as amount
    from entries
    group by year(ts)
    union all
    select '21' as rk, '&nbsp;&nbsp;Unearned earnings' as item, year(ts) as period, sum(case when cast(account as varchar) like '2%' then amount end) as amount
    from entries
    group by year(ts)
    union all
    select '3' as rk, '<b>Capital buffer</b>' as item, year(ts) as period, sum(case when cast(account as varchar) like '3%' then amount end) as amount
    from entries
    group by year(ts)
    union all
    select '31' as rk, '&nbsp;&nbsp;Issued as payment' as item, year(ts) as period, sum(case when cast(account as varchar) like '31%' then amount end) as amount
    from entries
    group by year(ts)
    union all
    select '32' as rk, '&nbsp;&nbsp;Retained earnings' as item, year(ts) as period, sum(case when cast(account as varchar) like '32%' then amount end) as amount
    from entries
    group by year(ts)
    union all
    select '321' as rk, '&nbsp;&nbsp;&nbsp;&nbsp;<i>Operating revenues</i>' as item, year(ts) as period, sum(case when cast(account as varchar) like '321%' then amount end) as amount
    from entries
    group by year(ts)
    union all
    select '322' as rk, '&nbsp;&nbsp;&nbsp;&nbsp;<i>Operating expenses</i>' as item, year(ts) as period, sum(case when cast(account as varchar) like '322%' then amount end) as amount
    from entries
    group by year(ts)
    union all
    select '323' as rk, '&nbsp;&nbsp;&nbsp;&nbsp;<i>Financial earnings</i>' as item, year(ts) as period, sum(case when cast(account as varchar) like '323%' then amount end) as amount
    from entries
    group by year(ts)
),
balances as (
    select rk, item, period, sum(amount) over (partition by item order by period asc) as balance
    from items
),
pivot as (
    select rk, item, 
        sum(case when period = 2019 then balance end) as y_2019,
        sum(case when period = 2020 then balance end) as y_2020,
        sum(case when period = 2021 then balance end) as y_2021,
        sum(case when period = 2022 then balance end) as y_2022,
        sum(case when period = 2023 then balance end) as y_2023,
        sum(case when period = 2024 then balance end) as y_2024
    from balances
    group by 1, 2
)
select *
from pivot
order by rk asc