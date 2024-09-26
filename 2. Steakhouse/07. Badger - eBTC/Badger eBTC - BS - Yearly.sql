/*
-- @title: Badger eBTC - BS - Yearly
-- @author: Steakhouse Financial
-- @description: Yearly balance sheet
-- @notes: until Dune's pivot feature, this dashboard requires updating the pivot cte whenever there's a new year
-- @version:
    - 1.0 - 2024-09-13 - Initial version
*/

with
    bs_yearly as (
        select *
        from query_4087119 -- BS - Monthly Table
        where (extract(month from dt) = 12 or date_trunc('month', dt) = date_trunc('month', current_date)) -- yearly basis
        and fi_id != 1999
    ),
    bs_pivot as (
        select
            fi_id,
            item,
            --sum(case when date_trunc('year', dt) = date '2024-01-01' then value_usd else 0 end) as "2024 YTD"
            sum(case when date_trunc('year', dt) = date '2024-01-01' then value_usd_m else 0 end) as "2024 YTD"
            --sum(case when date_trunc('year', dt) = date '2024-01-01' then amount_base else 0 end) as "2024 YTD"
        from bs_yearly
        group by 1, 2
    )

select
    item,
    "2024 YTD"
from bs_pivot
order by fi_id asc