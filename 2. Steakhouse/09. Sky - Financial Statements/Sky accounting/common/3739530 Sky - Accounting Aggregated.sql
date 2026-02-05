/*
-- @title: Accounting Aggregated
-- @author: Steakhouse Financial
-- @description: Generates double-entry accounting for Sky aggregated by period and account id
-- @notes:
    Changes in accounting v1 -> v2:
        Returned Workforce Expenses     ->  from 31730 to 31721
        Direct MKR Token Expenses       ->  from 31810 to 32110
        Vested MKR Token Expenses       ->  from 33110 to 32120
        MKR Contra Equity               ->  from 34110 to 32210
        DS Pause Proxy                  ->  from 32810 to 33110
-- @version:
        1.0 - 2024-05-17 - Initial version
        2.0 - 2024-10-24 - Excluding current_date to enable monitoring dashboard checks accross multiple queries
        3.0 - 2025-05-25 - Remove current date exclusion
*/

with
    chart_of_accounts as (
        select cast(account_id as varchar) as account_id from query_3689733 -- Chart of Accounts
    ),
    periods as (
        select
            date(period) as period,
            extract(year from period) as year,
            extract(month from period) as month
        from (
            select period
            from unnest(sequence(date('2019-11-01'), date_trunc('month', current_date), interval '1' month)) as t(period)
        )
    ),
    accounting as (
        select
            year(acc.ts) AS year,
            month(acc.ts) AS month,
            cast(acc.code as varchar) as account_id,
            acc.dai_value as value
        from dune.steakhouse.result_sky_accounting acc
        where dt < current_date
        union all
        select
            year,
            month,
            coa.account_id,
            0 AS value
        from periods
        cross join chart_of_accounts coa
    ),
    accounting_agg as (
        select
            year,
            month,
            account_id,
            sum(coalesce(value, 0)) AS sum_value
        from accounting
        group by 1, 2, 3
    ),
    -- cumulative liquidation revenues & expenses. In next CTE (accounting_net): if positive, sum into revenues; if negative, sum into expenses
    accounting_liq as (
        select distinct
            year,
            month,
            sum(coalesce(sum_value, 0)) over (partition by year, month) as liq_cum
        from accounting_agg
        where account_id in (
            '31210', -- Liquidation Revenues
            '31620'  -- Liquidation Expenses
        )
    ),
    accounting_net AS (
        select
            a.year,
            a.month,
            a.account_id,
            case
                when account_id = '31210' then if(liq_cum > 0, liq_cum, 0)
                when account_id = '31620' then if(liq_cum > 0, 0, liq_cum)
                else coalesce(sum_value, 0)
            end as sum_value,
            sum(case
                when account_id = '31210' then if(liq_cum > 0, liq_cum, 0)
                when account_id = '31620' then if(liq_cum > 0, 0, liq_cum)
                else coalesce(sum_value, 0)
            end) over (partition by a.account_id order by a.year, a.month) as cum_value
        from accounting_agg a
        left join accounting_liq l
            on a.year = l.year and a.month = l.month
    )
    
select * from accounting_net