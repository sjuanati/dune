/*
-- @title: ENS - Endowment
-- @description: Calculates the daily balance of the ENS Endowment Wallet by accounting category.
-- @author: Steakhouse Financial
-- @notes:
    - ENS Endowment wallet created on Jan-20-2023
-- @version:
    - 2.0 - 2024-02-19 - Added comment header, added contract & wallet description
    - 1.0 - 2023-08-10 - Initial version
*/

with entries as (
    select cast(account as varchar) account, ts, amount
    from dune.steakhouse.result_ens_accounting_main
    where wallet = '0x4f2083f5fbede34c2714affb3105539775f7fe64' -- ENS EnDAOment wallet
      and cast(account as varchar) like '1%' 
),
accounts as (   
    select * from query_2181835 -- ENS Chart of Accounts
),
items as (
    select account, date(ts) as period, amount as amount
    from entries
    union all
    select account, period, null as amount
    from (select distinct account from entries) items
    cross join (select distinct date(ts) as period from entries) periods
),
group_by as (
    select account, period, sum(amount) as amount
    from items
    group by 1, 2
),
balances as (
    select account, period, sum(amount) over (partition by account order by period asc) as balance
    from group_by
)

select
    coalesce(account_label, account) as item,
    cast(period as timestamp) as period,
    case when abs(balance) < 1 then 0 else balance end as balance
from balances
left join accounts using (account)
order by account asc