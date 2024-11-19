with
    periods as (
        select dt
        from unnest(sequence(date('2019-11-01'), date_trunc('month', current_date), interval '1' month)) as t(dt)
    ),
    accounting as (
        select
            date_trunc('month', a.dt) as dt,
            cast(a.code as varchar) as code,
            a.token,
            c.primary_label,
            c.secondary_label,
            c.account_label,
            c.category_label,
            c.subcategory_label,
            a.ilk,
            sum(dai_value) as dai_value
        from dune.steakhouse.result_sky_accounting a
        inner join query_3689733 c on a.code = c.account_id -- chart of accounts
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9
    ),
    pnl as (
        select
            dt,
            a.code,
            if(
                a.code like '39%',
                'Currency Translation to Presentation Token (excludes MKR)',
                'Statement of changes in protocol surplus'
            ) as income_stmt1,
            case
                when a.code like '39%'
                then concat('Currency Translation to Presentation Token - ', a.token)
                
                when a.code like '314%' -- MKR Mints, MKR Burns
                  or a.code like '321%' -- Direct MKR Token Expenses, Vested MKR Token Expenses
                then 'Statement of changes in tokenholdings'
                
                else 'Net Protocol Income'
            end as income_stmt2,
            case
                when a.code like '39%'
                then concat('Currency Translation to Presentation Token - ', a.token)

                when a.code like '313%' -- Trading Revenues
                then 'Net Trading Revenues'
                
                when a.code like '314%' -- MKR Mints, MKR Burns
                then a.account_label

                when a.code like '315%' -- Sin
                then 'Sin Flows'

                when a.code like '311%' -- Gross Interest Revenues
                  or (a.code like '316%' and a.code != '31620') -- DSR, Oracle Gas Expenses
                then 'Net Interest Revenue'
                
                when a.code like '312%'  -- Liquidation Revenues
                  or a.code like '31620' -- Liquidation Expenses
                then 'Net Liquidation Revenues'
                
                when a.code like '317%' -- Indirect Expenses
                then 'Total Indirect Expenses'

                when a.code like '321%' -- Direct MKR Token Expenses, Vested MKR Token Expenses
                then 'MKR Expenses'

                when a.code like '331%' -- Holdings
                then 'Treasury Assets'

                else '(!) undefined'
            end as income_stmt3,
            if(
                a.code like '39%',
                concat(a.account_label, ' - ', a.token),
                a.account_label
            ) as income_stmt4,
            if(
                a.code like '39%',
                concat(a.category_label, ' - ', a.token),
                a.category_label
            ) as income_stmt5,
            if(
                a.code like '39%',
                concat(a.subcategory_label, ' - ', a.token),
                a.subcategory_label
            ) as income_stmt6,
            a.ilk,
            dai_value
        from periods p
        left join accounting a using (dt)
        where a.code like '3%' -- Equity
          and a.code not like '322%' -- contra-equity
    ),
    pnl_index as (
        select
            concat_ws(
                '', -- no separator
                income_stmt2,
                income_stmt3,
                income_stmt4,
                income_stmt5,
                income_stmt6,
                ilk
            ) as search_key,
            *
        from pnl
    ),
    -- L0 aggregation at global level (total PNL)
    pnl_l0_aggr as (
        select
            'Total' as search_key,
            dt,
            null as code,
            'Total' as income_stmt1,
            null as income_stmt2,
            null as income_stmt3,
            null as income_stmt4,
            null as income_stmt5,
            null as income_stmt6,
            null as ilk,
            sum(dai_value) as dai_value
        from pnl_index
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ),
    -- L1 aggregation at <income_stmt1> level
    pnl_l1_aggr as (
        select
            concat(income_stmt1, 'Total') as search_key,
            dt,
            null as code,
            income_stmt1,
            'Total' as income_stmt2,
            null as income_stmt3,
            null as income_stmt4,
            null as income_stmt5,
            null as income_stmt6,
            null as ilk,
            sum(dai_value) as dai_value
        from pnl_index
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ),
    -- L2 aggregation at <income_stmt2> level
    pnl_l2_aggr as (
        select
            concat(income_stmt2, 'Total') as search_key,
            dt,
            null as code,
            income_stmt1,
            income_stmt2,
            'Total' as income_stmt3,
            null as income_stmt4,
            null as income_stmt5,
            null as income_stmt6,
            null as ilk,
            sum(dai_value) as dai_value
        from pnl_index
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ),
    -- L3 aggregation at <income_stmt3> level
    pnl_l3_aggr as (
        select
            concat(income_stmt2, income_stmt3, 'Total') as search_key,
            dt,
            null as code,
            income_stmt1,
            income_stmt2,
            income_stmt3,
            'Total' as income_stmt4,
            null as income_stmt5,
            null as income_stmt6,
            null as ilk,
            sum(dai_value) as dai_value
        from pnl_index
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ),
    -- L4 aggregation at <income_stmt4> level
    pnl_l4_aggr as (
        select
            concat(income_stmt2, income_stmt3, income_stmt4, 'Total') as search_key,
            dt,
            null as code,
            income_stmt1,
            income_stmt2,
            income_stmt3,
            income_stmt4,
            'Total' as income_stmt5,
            null as income_stmt6,
            null as ilk,
            sum(dai_value) as dai_value
        from pnl_index
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ),
    -- L5 aggregation at <income_stmt5> level
    pnl_l5_aggr as (
        select
            concat(income_stmt2, income_stmt3, income_stmt4, income_stmt5, 'Total') as search_key,
            dt,
            null as code,
            income_stmt1,
            income_stmt2,
            income_stmt3,
            income_stmt4,
            income_stmt5,
            'Total' as income_stmt6,
            null as ilk,
            sum(dai_value) as dai_value
        from pnl_index
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ),
    -- L6 aggregation at <income_stmt6> level
    pnl_l6_aggr as (
        select
            concat(income_stmt2, income_stmt3, income_stmt4, income_stmt5, income_stmt6, 'Total') as search_key,
            dt,
            null as code,
            income_stmt1,
            income_stmt2,
            income_stmt3,
            income_stmt4,
            income_stmt5,
            income_stmt6,
            'Total' as ilk,
            sum(dai_value) as dai_value
        from pnl_index
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ),
    totals as (
        select * from pnl_index
        union all
        select * from pnl_l0_aggr
        union all
        select * from pnl_l1_aggr
        union all
        select * from pnl_l2_aggr
        union all
        select * from pnl_l3_aggr
        union all
        select * from pnl_l4_aggr
        union all
        select * from pnl_l5_aggr
        union all
        select * from pnl_l6_aggr
    ),
    pivot as (
        select
            search_key,
            income_stmt1,
            income_stmt2,
            income_stmt3,
            income_stmt4,
            income_stmt5,
            income_stmt6,
            ilk,
            sum(case when date_trunc('year', dt) = date '2024-01-01' then dai_value else null end) as "2024",
            --max(case when dt = date '2024-12-01' then dai_value else null end) as "2024-12",
            --max(case when dt = date '2024-11-01' then dai_value else null end) as "2024-11",
            max(case when dt = date '2024-10-01' then dai_value else null end) as "2024-10",
            max(case when dt = date '2024-09-01' then dai_value else null end) as "2024-09",  
            max(case when dt = date '2024-08-01' then dai_value else null end) as "2024-08",
            max(case when dt = date '2024-07-01' then dai_value else null end) as "2024-07",
            max(case when dt = date '2024-06-01' then dai_value else null end) as "2024-06",
            max(case when dt = date '2024-05-01' then dai_value else null end) as "2024-05",
            max(case when dt = date '2024-04-01' then dai_value else null end) as "2024-04",
            max(case when dt = date '2024-03-01' then dai_value else null end) as "2024-03",
            max(case when dt = date '2024-02-01' then dai_value else null end) as "2024-02",
            max(case when dt = date '2024-01-01' then dai_value else null end) as "2024-01",
            sum(case when date_trunc('year', dt) = date '2023-01-01' then dai_value else null end) as "2023",
            max(case when dt = date '2023-12-01' then dai_value else null end) as "2023-12",
            max(case when dt = date '2023-11-01' then dai_value else null end) as "2023-11",
            max(case when dt = date '2023-10-01' then dai_value else null end) as "2023-10",
            max(case when dt = date '2023-09-01' then dai_value else null end) as "2023-09",
            max(case when dt = date '2023-08-01' then dai_value else null end) as "2023-08",
            max(case when dt = date '2023-07-01' then dai_value else null end) as "2023-07",
            max(case when dt = date '2023-06-01' then dai_value else null end) as "2023-06",
            max(case when dt = date '2023-05-01' then dai_value else null end) as "2023-05",
            max(case when dt = date '2023-04-01' then dai_value else null end) as "2023-04",
            max(case when dt = date '2023-03-01' then dai_value else null end) as "2023-03",
            max(case when dt = date '2023-02-01' then dai_value else null end) as "2023-02",
            max(case when dt = date '2023-01-01' then dai_value else null end) as "2023-01",
            sum(case when date_trunc('year', dt) = date '2022-01-01' then dai_value else null end) as "2022",
            max(case when dt = date '2022-12-01' then dai_value else null end) as "2022-12",
            max(case when dt = date '2022-11-01' then dai_value else null end) as "2022-11",
            max(case when dt = date '2022-10-01' then dai_value else null end) as "2022-10",
            max(case when dt = date '2022-09-01' then dai_value else null end) as "2022-09",
            max(case when dt = date '2022-08-01' then dai_value else null end) as "2022-08",
            max(case when dt = date '2022-07-01' then dai_value else null end) as "2022-07",
            max(case when dt = date '2022-06-01' then dai_value else null end) as "2022-06",
            max(case when dt = date '2022-05-01' then dai_value else null end) as "2022-05",
            max(case when dt = date '2022-04-01' then dai_value else null end) as "2022-04",
            max(case when dt = date '2022-03-01' then dai_value else null end) as "2022-03",
            max(case when dt = date '2022-02-01' then dai_value else null end) as "2022-02",
            max(case when dt = date '2022-01-01' then dai_value else null end) as "2022-01",
            sum(case when date_trunc('year', dt) = date '2021-01-01' then dai_value else null end) as "2021",
            max(case when dt = date '2021-12-01' then dai_value else null end) as "2021-12",
            max(case when dt = date '2021-11-01' then dai_value else null end) as "2021-11",
            max(case when dt = date '2021-10-01' then dai_value else null end) as "2021-10",
            max(case when dt = date '2021-09-01' then dai_value else null end) as "2021-09",
            max(case when dt = date '2021-08-01' then dai_value else null end) as "2021-08",
            max(case when dt = date '2021-07-01' then dai_value else null end) as "2021-07",
            max(case when dt = date '2021-06-01' then dai_value else null end) as "2021-06",
            max(case when dt = date '2021-05-01' then dai_value else null end) as "2021-05",
            max(case when dt = date '2021-04-01' then dai_value else null end) as "2021-04",
            max(case when dt = date '2021-03-01' then dai_value else null end) as "2021-03",
            max(case when dt = date '2021-02-01' then dai_value else null end) as "2021-02",
            max(case when dt = date '2021-01-01' then dai_value else null end) as "2021-01",
            sum(case when date_trunc('year', dt) = date '2020-01-01' then dai_value else null end) as "2020",
            max(case when dt = date '2020-12-01' then dai_value else null end) as "2020-12",
            max(case when dt = date '2020-11-01' then dai_value else null end) as "2020-11",
            max(case when dt = date '2020-10-01' then dai_value else null end) as "2020-10",
            max(case when dt = date '2020-09-01' then dai_value else null end) as "2020-09",
            max(case when dt = date '2020-08-01' then dai_value else null end) as "2020-08",
            max(case when dt = date '2020-07-01' then dai_value else null end) as "2020-07",
            max(case when dt = date '2020-06-01' then dai_value else null end) as "2020-06",
            max(case when dt = date '2020-05-01' then dai_value else null end) as "2020-05",
            max(case when dt = date '2020-04-01' then dai_value else null end) as "2020-04",
            max(case when dt = date '2020-03-01' then dai_value else null end) as "2020-03",
            max(case when dt = date '2020-02-01' then dai_value else null end) as "2020-02",
            max(case when dt = date '2020-01-01' then dai_value else null end) as "2020-01",
            sum(case when date_trunc('year', dt) = date '2019-01-01' then dai_value else null end) as "2019",
            max(case when dt = date '2019-12-01' then dai_value else null end) as "2019-12",
            max(case when dt = date '2019-11-01' then dai_value else null end) as "2019-11"
        FROM totals
        group by 1, 2, 3, 4, 5, 6, 7, 8
    )

select * from pivot order by income_stmt1 desc, income_stmt2 desc

/*
-- Test query
select search_key, income_stmt1, income_stmt2, income_stmt3, income_stmt4, income_stmt5, income_stmt6, ilk, dai_value
from totals
where dt = date '2024-05-01'
  and ilk = 'Total'
  --and 'Total' not in (income_stmt1, income_stmt2, income_stmt3, income_stmt4, income_stmt5, income_stmt6)
  --and ilk is null
  --and income_stmt6 = 'Total'
order by income_stmt2 desc
*/