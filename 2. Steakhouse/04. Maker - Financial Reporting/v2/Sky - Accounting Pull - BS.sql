with
    coa as (
        select distinct
            a.code,
            a.ilk,
            c.primary_label,
            c.secondary_label,
            c.account_label,
            c.category_label,
            c.subcategory_label
        from dune.steakhouse.result_sky_accounting a
        inner join query_3689733 c on a.code = c.account_id -- chart of accounts
    ),
    periods as (
        select
            period,
            c.*
        from unnest(sequence(DATE('2019-11-01'), date_trunc('month', current_date), interval '1' month)) AS t(period)
        cross join coa c
    ),
    acc_main as (
        select
            p.period,
            p.code as account_id,
            p.primary_label,
            if(
                p.code in (19999, 39999),
                'Currency Translation to Presentation Token',
                'Values'
            ) as currency_translation_label,
            p.secondary_label,
            p.account_label,
            p.category_label,
            p.subcategory_label,
            p.ilk,
            sum(sum(coalesce(a.dai_value, 0))) over (partition by p.code, p.ilk order by p.period asc) as value_cum
        from periods p
        left join dune.steakhouse.result_sky_accounting a
            on p.period = date_trunc('month', a.dt)
            and p.code = a.code
            and (p.ilk = a.ilk or (p.ilk is null and a.ilk is null)) -- handle null ilks
        group by 1,2,3,4,5,6,7,8,9
    ),
    -- <Currency Translation to Presentation Token> by token in assets & equity
    acc_m2m_by_token as (
        select
            p.period,
            p.code as account_id,
            p.primary_label,
            'Currency Translation to Presentation Token' as currency_translation_label,
            concat(p.secondary_label, ' - ', a.token) as secondary_label,
            concat(p.account_label, ' - ', a.token) as account_label,
            concat(p.category_label, ' - ', a.token) as category_label,
            concat(p.subcategory_label, ' - ', a.token) as subcategory_label,
            p.ilk,
            sum(sum(coalesce(a.dai_value, 0))) over (partition by p.code, concat(p.secondary_label, ' - ', a.token) order by p.period asc) as value_cum
        from periods p
        left join dune.steakhouse.result_sky_accounting a
            on p.period = date_trunc('month', a.dt)
            and p.code = a.code
        where p.code in (19999, 39999)
        group by 1,2,3,4,5,6,7,8,9
    ),
    acc_union as (
        select
            7 as ranking,
            concat_ws(
                '', -- no separator
                currency_translation_label,
                secondary_label,
                account_label,
                category_label,
                subcategory_label,
                ilk
            ) as search_key,
            *
        from (
            select * from acc_main -- if(value_cum < 1e-6, 0, value_cum) as value_cum 
            union all
            select * from acc_m2m_by_token
        )
    ),
    -- L1 aggregation at <primary_label> level
    acc_l1_aggr as (
        select
            1 as ranking,
            'Total' as search_key,
            period,
            null as account_id,
            primary_label,
            null as currency_translation_label,
            'Total' as secondary_label,
            null as account_label,
            null as category_label,
            null as subcategory_label,
            null as ilk,
            sum(value_cum) as value_cum
        from acc_union
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
    ),
    -- L2 aggregation at <currency_translation_label> level
    acc_l2_aggr as (
        select
            2 as ranking,
            concat(currency_translation_label, 'Total') as search_key,
            period,
            null as account_id,
            primary_label,
            currency_translation_label,
            'Total' as secondary_label,
            null as account_label,
            null as category_label,
            null as subcategory_label,
            null as ilk,
            sum(value_cum) as value_cum
        from acc_union
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
    ),
    -- L3 aggregation at <secondary_label> level
    acc_l3_aggr as (
        select
            3 as ranking,
            concat(currency_translation_label, secondary_label, 'Total') as search_key,
            period,
            null as account_id,
            primary_label,
            currency_translation_label,
            secondary_label,
            'Total' as account_label,
            null as category_label,
            null as subcategory_label,
            null as ilk,
            sum(value_cum) as value_cum
        from acc_union
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
    ),
    -- L4 aggregation at <account_label> level
    acc_l4_aggr as (
        select
            4 as ranking,
            concat(currency_translation_label, secondary_label, account_label, 'Total') as search_key,
            period,
            null as account_id,
            primary_label,
            currency_translation_label,
            secondary_label,
            account_label,
            'Total' as category_label,
            null as subcategory_label,
            null as ilk,
            sum(value_cum) as value_cum
        from acc_union
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
    ),
    -- L5 aggregation at <category_label> level
    acc_l5_aggr as (
        select
            5 as ranking,
            concat(currency_translation_label, secondary_label, account_label, category_label, 'Total') as search_key,
            period,
            null as account_id,
            primary_label,
            currency_translation_label,
            secondary_label,
            account_label,
            category_label,
            'Total' as subcategory_label,
            null as ilk,
            sum(value_cum) as value_cum
        from acc_union
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
    ),
    -- L6 aggregation at <subcategory_label> level
    acc_l6_aggr as (
        select
            6 as ranking,
            concat(currency_translation_label, secondary_label, account_label, category_label, subcategory_label, 'Total') as search_key,
            period,
            null as account_id,
            primary_label,
            currency_translation_label,
            secondary_label,
            account_label,
            category_label,
            subcategory_label,
            'Total' as ilk,
            sum(value_cum) as value_cum
        from acc_union
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
    ),
    totals as (
        select * from acc_union
        union all
        select * from acc_l1_aggr
        union all
        select * from acc_l2_aggr
        union all
        select * from acc_l3_aggr
        union all
        select * from acc_l4_aggr
        union all
        select * from acc_l5_aggr
        union all
        select * from acc_l6_aggr
    ),
    pivot as (
        select
            ranking,
            search_key,
            account_id,
            primary_label,
            currency_translation_label,
            secondary_label,
            account_label,
            category_label,
            subcategory_label,
            ilk,
            max(case when period = date '2024-10-01' then value_cum else null end) as "2024",
            max(case when period = date '2024-10-01' then value_cum else null end) as "2024-10",
            max(case when period = date '2024-09-01' then value_cum else null end) as "2024-09",  
            max(case when period = date '2024-08-01' then value_cum else null end) as "2024-08",
            max(case when period = date '2024-07-01' then value_cum else null end) as "2024-07",
            max(case when period = date '2024-06-01' then value_cum else null end) as "2024-06",
            max(case when period = date '2024-05-01' then value_cum else null end) as "2024-05",
            max(case when period = date '2024-04-01' then value_cum else null end) as "2024-04",
            max(case when period = date '2024-03-01' then value_cum else null end) as "2024-03",
            max(case when period = date '2024-02-01' then value_cum else null end) as "2024-02",
            max(case when period = date '2024-01-01' then value_cum else null end) as "2024-01",
            max(case when period = date '2023-12-01' then value_cum else null end) as "2023",
            max(case when period = date '2023-12-01' then value_cum else null end) as "2023-12",
            max(case when period = date '2023-11-01' then value_cum else null end) as "2023-11",
            max(case when period = date '2023-10-01' then value_cum else null end) as "2023-10",
            max(case when period = date '2023-09-01' then value_cum else null end) as "2023-09",
            max(case when period = date '2023-08-01' then value_cum else null end) as "2023-08",
            max(case when period = date '2023-07-01' then value_cum else null end) as "2023-07",
            max(case when period = date '2023-06-01' then value_cum else null end) as "2023-06",
            max(case when period = date '2023-05-01' then value_cum else null end) as "2023-05",
            max(case when period = date '2023-04-01' then value_cum else null end) as "2023-04",
            max(case when period = date '2023-03-01' then value_cum else null end) as "2023-03",
            max(case when period = date '2023-02-01' then value_cum else null end) as "2023-02",
            max(case when period = date '2023-01-01' then value_cum else null end) as "2023-01",
            max(case when period = date '2022-12-01' then value_cum else null end) as "2022",
            max(case when period = date '2022-12-01' then value_cum else null end) as "2022-12",
            max(case when period = date '2022-11-01' then value_cum else null end) as "2022-11",
            max(case when period = date '2022-10-01' then value_cum else null end) as "2022-10",
            max(case when period = date '2022-09-01' then value_cum else null end) as "2022-09",
            max(case when period = date '2022-08-01' then value_cum else null end) as "2022-08",
            max(case when period = date '2022-07-01' then value_cum else null end) as "2022-07",
            max(case when period = date '2022-06-01' then value_cum else null end) as "2022-06",
            max(case when period = date '2022-05-01' then value_cum else null end) as "2022-05",
            max(case when period = date '2022-04-01' then value_cum else null end) as "2022-04",
            max(case when period = date '2022-03-01' then value_cum else null end) as "2022-03",
            max(case when period = date '2022-02-01' then value_cum else null end) as "2022-02",
            max(case when period = date '2022-01-01' then value_cum else null end) as "2022-01",
            max(case when period = date '2021-12-01' then value_cum else null end) as "2021",
            max(case when period = date '2021-12-01' then value_cum else null end) as "2021-12",
            max(case when period = date '2021-11-01' then value_cum else null end) as "2021-11",
            max(case when period = date '2021-10-01' then value_cum else null end) as "2021-10",
            max(case when period = date '2021-09-01' then value_cum else null end) as "2021-09",
            max(case when period = date '2021-08-01' then value_cum else null end) as "2021-08",
            max(case when period = date '2021-07-01' then value_cum else null end) as "2021-07",
            max(case when period = date '2021-06-01' then value_cum else null end) as "2021-06",
            max(case when period = date '2021-05-01' then value_cum else null end) as "2021-05",
            max(case when period = date '2021-04-01' then value_cum else null end) as "2021-04",
            max(case when period = date '2021-03-01' then value_cum else null end) as "2021-03",
            max(case when period = date '2021-02-01' then value_cum else null end) as "2021-02",
            max(case when period = date '2021-01-01' then value_cum else null end) as "2021-01",
            max(case when period = date '2020-12-01' then value_cum else null end) as "2020",
            max(case when period = date '2020-12-01' then value_cum else null end) as "2020-12",
            max(case when period = date '2020-11-01' then value_cum else null end) as "2020-11",
            max(case when period = date '2020-10-01' then value_cum else null end) as "2020-10",
            max(case when period = date '2020-09-01' then value_cum else null end) as "2020-09",
            max(case when period = date '2020-08-01' then value_cum else null end) as "2020-08",
            max(case when period = date '2020-07-01' then value_cum else null end) as "2020-07",
            max(case when period = date '2020-06-01' then value_cum else null end) as "2020-06",
            max(case when period = date '2020-05-01' then value_cum else null end) as "2020-05",
            max(case when period = date '2020-04-01' then value_cum else null end) as "2020-04",
            max(case when period = date '2020-03-01' then value_cum else null end) as "2020-03",
            max(case when period = date '2020-02-01' then value_cum else null end) as "2020-02",
            max(case when period = date '2020-01-01' then value_cum else null end) as "2020-01",
            max(case when period = date '2019-12-01' then value_cum else null end) as "2019",
            max(case when period = date '2019-12-01' then value_cum else null end) as "2019-12",
            max(case when period = date '2019-11-01' then value_cum else null end) as "2019-11"
        FROM totals
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    )

select *
from pivot
order by primary_label asc, ranking asc, currency_translation_label desc, secondary_label asc, category_label asc, subcategory_label asc
