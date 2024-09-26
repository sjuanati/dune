/*
-- @title: Badger eBTC - BS - Yearly
-- @author: Steakhouse Financial
-- @description: Yearly balance sheet
-- @notes: until Dune's pivot feature, this dashboard requires updating the pivot cte whenever there's a new year
-- @version:
    - 1.0 - 2024-09-13 - Initial version
*/

with
    bs as (
        select
            dt,
            fi_id,
            '&nbsp;&nbsp;' || item as item,
            abs(value_usd) as value_usd,
            abs(value_usd_m) as value_usd_m,
            abs(amount_base) as amount_base
        from query_4045909 -- BS - Monthly
    ),
    bs_headers as (
        select
            dt,
            1999 as fi_id,
            '<b>' || 'Balance Sheet' || '</b>' as item,
            null as value_usd,
            null as value_usd_m,
            null as amount_base
        from unnest(sequence(date '2024-03-01', current_date, interval '1' month)) as t(dt)
        union all
        select
            dt,
            fi_id,
            '&nbsp;&nbsp;' || item as item,
            value_usd,
            value_usd_m,
            amount_base
        from bs
        union all
        select
            dt,
            2000 as fi_id,
            '&nbsp;&nbsp;<b>' || 'Assets' || '</b>' as item,
            sum(value_usd),
            sum(value_usd_m),
            sum(amount_base)
        from bs
        where fi_id < 2050
        group by 1, 2
        union all
        select
            dt,
            2300 as fi_id,
            '&nbsp;&nbsp;<b>' || 'Liabilities' || '</b>' as item,
            sum(value_usd),
            sum(value_usd_m),
            sum(amount_base)
        from bs
        where fi_id >= 2400
        and fi_id < 2500
        group by 1, 2
        union all
        select
            dt,
            2500 as fi_id,
            '&nbsp;&nbsp;<b>' || 'Equity' || '</b>' as item,
            sum(value_usd),
            sum(value_usd_m),
            sum(amount_base)
        from bs
        where fi_id >= 2500
        group by 1, 2
    )

select *
from bs_headers
order by dt asc, fi_id asc