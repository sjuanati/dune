with alm_assets as (
    select period, alm_category, sum(exposure) as exposure, sum(capital_at_risk) as capital_at_risk, 
        sum(liquidity_day) as liquidity_day, sum(liquidity_week) as liquidity_week,
        sum(liquidity_month) as liquidity_month, sum(liquidity_year) as liquidity_year,
        sum(duration_risk) as duration_risk, sum(credit_risk) as credit_risk,
        sum(crypto_market_risk) as crypto_market_risk, sum(operational_risk) as operational_risk,
        sum(rate*exposure) as annualized_revenues
    from query_3273691
    group by 1, 2
)
select period, alm_category, exposure, 
    annualized_revenues, annualized_revenues/exposure as rev_ratio,
    capital_at_risk, capital_at_risk/exposure as car_ratio,
    liquidity_day, liquidity_week, liquidity_month, liquidity_year,
    liquidity_day/exposure as liquidity_day_ratio,
    liquidity_week/exposure as liquidity_week_ratio,
    liquidity_month/exposure as liquidity_month_ratio,
    liquidity_year/exposure as liquidity_year_ratio,
    duration_risk, credit_risk,
    crypto_market_risk, operational_risk,
    round(duration_risk/exposure,4) as duration_risk_ratio,
    round(credit_risk/exposure,4) as credit_risk_ratio,
    round(crypto_market_risk/exposure,4) as crypto_market_risk_ratio,
    round(operational_risk/exposure,4) as operational_risk_ratio
from alm_assets
where period > current_date - interval '180' day
order by 1 desc, 2 desc