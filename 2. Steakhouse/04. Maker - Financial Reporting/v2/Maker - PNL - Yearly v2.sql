/*
-- @title: Maker - Profit & Loss - Yearly
-- @author: Steakhouse Financial
-- @description: Yearly income statement since 2020
-- @version:
    - 1.0 - 2024-06-05 - Initial version
    - 2.0 - 2024-07-13 - Updated to sum monthly data for yearly totals instead of using year-end snapshots
                        -> Updated substring of period to year variable
*/

with
    pnl as (
        select
            fi_id,
            period,
            substr(period, 1, 4) as year,
            case
                when fi_id in (1001, 1600, 1650) then label
                when fi_id = 1250 then '&nbsp;&nbsp;Interest Revenues' 
                when fi_id = 1375 then '&nbsp;&nbsp;DSR'
                when fi_id in (1510, 1630) then '&nbsp;&nbsp;<b>' || label || '</b>'
                else '&nbsp;&nbsp;' || label
            end as item,
            value
        from query_3735842 -- Maker - Profit & Loss
        where fi_id in (
            1001, -- Revenues
            1100, -- Trading Revenues
            1200, -- Liquidations Revenues
            1250, -- Gross Interest Revenues
            1375, -- Direct Expenses
            1510, -- Net Revenues
            1600, -- Operating expenses
            1610, -- Direct to Third Party Expenses
            1615, -- Keeper Maintenance
            1620, -- Workforce Expenses
            1625, -- MKR Token Expenses
            1630, -- Operating Expenses
            1650  --Net Operating Earnings
        )
    ),
    pivot as (
        select
            fi_id,
            item,
            case when fi_id in (1001, 1600) then null
                 else sum(case when year = '2024' then value else 0 end)
            end as "2024 YTD",
            case when fi_id in (1001, 1600) then null
                 else sum(case when year = '2023' then value else 0 end)
            end as "2023",
            case when fi_id in (1001, 1600) then null
                 else sum(case when year = '2022' then value else 0 end)
            end as "2022",
            case when fi_id in (1001, 1600) then null
                 else sum(case when year = '2021' then value else 0 end)
            end as "2021",
            case when fi_id in (1001, 1600) then null
                 else sum(case when year = '2020' then value else 0 end)
            end as "2020"
        from pnl
        group by fi_id, item
    )

select
    item,
    "2024 YTD",
    "2023",
    "2022",
    "2021",
    "2020"
from pivot
order by fi_id asc