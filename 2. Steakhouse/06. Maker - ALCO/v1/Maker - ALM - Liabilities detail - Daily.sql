with liabilities as (
    select date_trunc('day', ts) as period, 'DAI' as alm_sub_category, 'Liabilities' as alm_category, sum(dai_value) as amount
    from dune.steakhouse.result_maker_accounting
    where cast(code as varchar) like '2112%'
    group by 1
    union all
    select date_trunc('day', ts) as period, 'DSR' as alm_sub_category, 'Liabilities' as alm_category, sum(dai_value) as amount
    from dune.steakhouse.result_maker_accounting
    where cast(code as varchar) like '2111%'
    group by 1
    union all
    select date_trunc('day', ts) as period, 'Surplus Buffer' as alm_sub_category, 'Equity' as alm_category, sum(dai_value) as amount
    from dune.steakhouse.result_maker_accounting
    where cast(code as varchar) like '31%' and cast(code as varchar) not like '318%'
    group by 1
),
filler as (
    select *
    from liabilities
    union all
    select period, alm_sub_category, alm_category, null
    from (select distinct alm_sub_category, alm_category from liabilities) as liabilities
    cross join unnest(sequence(date'2020-01-01', current_date, interval '1' day)) as t(period)
),
grp_by as (
    select period, alm_sub_category, alm_category, sum(amount) as amount
    from filler
    group by 1, 2, 3
),
balances as (
    select period, alm_sub_category, alm_category, sum(amount) over (partition by alm_sub_category order by period asc) as balance
    from grp_by
)
select period, alm_sub_category,  alm_category, balance
from balances
order by period desc, balance desc
