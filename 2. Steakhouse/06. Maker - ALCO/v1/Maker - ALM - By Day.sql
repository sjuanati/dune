with alm_assets as (
    select period, sum(exposure) as exposure, sum(capital_at_risk) as capital_at_risk, 
        sum(liquidity_day) as liquidity_day, sum(liquidity_week) as liquidity_week,
        sum(liquidity_month) as liquidity_month, sum(liquidity_year) as liquidity_year,
        sum(duration_risk) as duration_risk,
        sum(credit_risk) as credit_risk,
        sum(crypto_market_risk) as crypto_market_risk,
        sum(operational_risk) as operational_risk,
        sum(exposure*rate) as annualized_revenues
    from query_3273691
    group by 1
),
alm_liabilities as (
    select period, sum(balance) as total_liabilities, 
        sum(case when alm_category = 'Equity' then balance end) as equity,
        sum(case when alm_category = 'Liabilities' then balance end) as total_dai,
        sum(case when alm_sub_category = 'DSR' then balance end) as dsr
    from query_3293442
    group by 1
),
dsr_rate as (
    select period, dsr_rate from query_3583845  -- dsr rate per day
),
mkr_price as (
    select period, mkr_price from query_3584212 -- MKR price per day
),
mkr_circulating as (
    select period, balance as mkr_circulating from query_482349 -- MKR outstanding
),
dai_maturity as (
    select cast(dt as timestamp) as period,
        sum(case when maturity in ('1-block', '1-day') then outflow end) as outflow_day,
        sum(case when maturity in ('1-week') then outflow end) as outflow_week,
        sum(case when maturity in ('1-month', '3-months') then outflow end) as outflow_month,
        sum(case when maturity in ('1-year') then outflow end) as outflow_year
    from query_907852
    group by 1
)
select period, exposure, capital_at_risk, capital_at_risk/exposure as car_ratio,
    duration_risk, credit_risk, crypto_market_risk, operational_risk,
    duration_risk/exposure as duration_risk_ratio,
    credit_risk/exposure as credit_risk_ratio,
    crypto_market_risk/exposure as crypto_market_risk_risk,
    operational_risk/exposure as operational_risk_ratio,
    liquidity_day, liquidity_week, liquidity_month, liquidity_year,
    liquidity_day/exposure as liquidity_day_ratio,
    liquidity_week/exposure as liquidity_week_ratio,
    liquidity_month/exposure as liquidity_month_ratio,
    liquidity_year/exposure as liquidity_year_ratio,
    equity,
    100*equity as equity_pct,
    total_dai,
    total_dai - dsr as dai_no_dsr,
    dsr,
    dsr/total_dai as dsr_ratio,
    100*dsr/total_dai as dsr_ratio_pct,
    equity - capital_at_risk as capital_after_risk,
    greatest(0, equity - capital_at_risk) as capital_after_risk_pos,
    least(0, equity - capital_at_risk) as capital_after_risk_neg,
    greatest(0, equity / capital_at_risk) as equity_coverage,
    100*greatest(0, equity / capital_at_risk) as equity_coverage_pct,
    1 as one,
     outflow_day,
     outflow_week,
     outflow_month,
     outflow_year,
     annualized_revenues,
     annualized_revenues/exposure as asset_yield,
     dsr_rate,
     dsr_rate*dsr as interest_expenses,
     dsr_rate*dsr/exposure as assets_funding_rate,
     (annualized_revenues - dsr_rate*dsr) as net_interest_income,
     (annualized_revenues - dsr_rate*dsr)/exposure as net_interest_margin,
     mkr_price * mkr_circulating as market_cap,
     exposure*0.02*40 as normalized_valuation_pe40,
     (annualized_revenues - dsr_rate*dsr)*40 as valuation_pe40,
     capital_at_risk*20 as car_x20
from alm_assets
inner join alm_liabilities using (period)
inner join dai_maturity using (period)
left join dsr_rate using (period)
left join mkr_price using (period)
left join mkr_circulating using (period)
order by 1 desc, 2 desc