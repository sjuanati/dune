-- Badger eBTC - Financial Statements

with
    financial_data as (
        select dt as period, fi_id, label_tab, value
        from "query_4065649(value='value_usd')" -- Profit & Loss
        union all
        select dt, fi_id, item as label_tab, value_usd as value
        from query_4087119 -- Balance Sheet
    ),
    pivot as (
        select
            fi_id,
            label_tab,
            max(case when period = date_trunc('month', date '2024-09-01') then value else null end) as "2024-09 ⏳", -- current
            max(case when period = date_trunc('month', date '2024-08-01') then value else null end) as "2024-08",
            max(case when period = date_trunc('month', date '2024-07-01') then value else null end) as "2024-07",
            max(case when period = date_trunc('month', date '2024-06-01') then value else null end) as "2024-06",
            max(case when period = date_trunc('month', date '2024-05-01') then value else null end) as "2024-05",
            max(case when period = date_trunc('month', date '2024-04-01') then value else null end) as "2024-04",
            max(case when period = date_trunc('month', date '2024-03-01') then value else null end) as "2024-03"
        FROM financial_data
        group by 1, 2
    )

select
    label_tab as item,
    "2024-09 ⏳",
    "2024-08",
    "2024-07",
    "2024-06",
    "2024-05",
    "2024-04",
    "2024-03"
from pivot
order by fi_id asc
