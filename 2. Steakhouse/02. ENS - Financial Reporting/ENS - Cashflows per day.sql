/*
-- @title: ENS - Cashflows per day
-- @description: Calculates daily cashflows for the last rolling year
-- @author: Steakhouse Financial
-- @notes: N/A
-- @version:
    - 2.0 - 2024-02-19 - Added comment header, updated query formatting
    - 1.0 - 2023-10-02 - Initial version
*/

with
    entries as (
        select * from query_2244104 -- ENS - Accounting - Main
    ),
    items as (
        select
            '1' as rk,
            'Cash In' as item,
            date_trunc('day', ts) as period,
            sum(case when cast(account as varchar) like '121%' then amount end) as amount
        from entries
        where ledger = 'CASH'
        group by 3
    )

select *
from items
where period >= current_date - interval '365' day