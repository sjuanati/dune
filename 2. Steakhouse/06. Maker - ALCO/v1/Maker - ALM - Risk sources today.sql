with data as (
    select * 
    from query_3286476
    where period = current_date
)
select 1 as norder, 'Duration' as risk_category, duration_risk as risk from data
union all
select 2 as norder, 'Credit' as risk_category, credit_risk as risk from data
union all
select 3 as norder, 'Market' as risk_category, crypto_market_risk as risk from data
union all
select 4 as norder, 'Operational' as risk_category, operational_risk as risk from data
order by 1 asc