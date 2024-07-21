/*
-- @title: Maker - Balance Sheet - Yearly
-- @author: Steakhouse Financial
-- @description: Calculates a simplified balance sheet on a yearly basis, grouping by assets,
                 liabilities, and capital, over the last 4 rolling years.
-- @notes: pivot cte needs to be updated every new January (Dune does not support dynamic pivot)
-- @version:
    - 1.0 - 2024-06-05 - Initial version
*/

with
    bs as (
        select * from query_3704439 -- Maker - Balance Sheet - Raw
    ),
    totals_ranked as (
        select 10 as rk, month, '<b>Assets</b>' as item, assets as value from bs
        union all
        select 11 as rk, month, '&nbsp;&nbsp;Crypto Vaults', total_crypto from bs
        union all
        select 12 as rk, month, '&nbsp;&nbsp;PSM Vaults', psm from bs
        union all
        select 13 as rk, month, '&nbsp;&nbsp;RWA Vaults', total_rwa from bs
        union all
        select 14 as rk, month, '&nbsp;&nbsp;Treasury Holdings', treasury from bs
        union all
        select 20 as rk, month, '<b>Liabilities</b>' as item, total_liabilities as value from bs
        union all
        select 21 as rk, month, '&nbsp;&nbsp;Dai Saving Rate (DSR)', dsr from bs
        union all
        select 22 as rk, month, '&nbsp;&nbsp;Dai in Circulation', dai from bs
        union all
        select 30 as rk, month, '<b>Equity</b>' as item, total_equity as value from bs
        union all
        select 31 as rk, month, '&nbsp;&nbsp;Surplus Buffer', surplus_buffer from bs
        union all
        select 32 as rk, month, '&nbsp;&nbsp;Treasury Holdings', treasury_holdings from bs
    ),
    pivot as (
        select
            rk,
            item,
            max(case when month = '2020-12' then coalesce(value, 0) else null end) as "2020",
            max(case when month = '2021-12' then coalesce(value, 0) else null end) as "2021",
            max(case when month = '2022-12' then coalesce(value, 0) else null end) as "2022",
            max(case when month = '2023-12' then coalesce(value, 0) else null end) as "2023",
            max(case when month = date_format(current_date, '%Y-%m') then coalesce(value, 0) else null end) as "2024 YTD"
        FROM totals_ranked
        group by rk, item
    )

select
    item,
    "2024 YTD",
    "2023",
    "2022",
    "2021",
    "2020"
from pivot
order by rk asc