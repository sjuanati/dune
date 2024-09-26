/*
-- @title: Badger eBTC - PNL - Yearly
-- @author: Steakhouse Financial
-- @description: Yearly income statement
-- @notes: until Dune's pivot feature, this dashboard requires updating the pivot cte whenever there's a new year
-- @version:
    - 1.0 - 2024-09-16 - Initial version
*/

with
    pnl as (
        select
            fi_id,
            dt,
            case
                -- no spacing, bold
                when fi_id in (
                    1005, -- Revenues
                    1370, -- Expenses
                    1500  -- Net Earnings
                ) then '<b>' || label || '</b>'
                -- spacing
                else '&nbsp;&nbsp;' || label
            end as item,
            value
        from query_4065649 -- Badger eBTC - PNL
        where fi_id in (
            1005, -- Revenues
            1100, -- Protocol Yield Share
            1125, -- Flash Loans
            1150, -- Redemptions
            1370, -- Expenses
            1400, -- Incentives
            1500  -- Net Earnings
        )
    ),
    pivot as (
        select
            fi_id,
            item,
            sum(case when date_trunc('year', dt) = date '2024-01-01' then coalesce(value, 0) else 0 end) as "2024 YTD"
        from pnl
        group by 1, 2
    )

select
    item,
    "2024 YTD"
from pivot
order by fi_id asc