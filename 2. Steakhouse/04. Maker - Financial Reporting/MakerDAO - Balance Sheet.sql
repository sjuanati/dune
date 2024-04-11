with entries as (
    select cast(code as varchar) as account, ts
    -- previously dai_value
    , value as amount from dune.steakhouse.result_maker_accounting
),
items as (
    select '11' as rk, 'Crypto-Loans' as item, date(ts) as period, sum(case when account like '11%' then amount end) as amount
    from entries
    group by date(ts)
    union all
    select '1231' as rk, 'Real-World Assets' as item, date(ts) as period, sum(case when account like '123%' then amount end) as amount
    from entries
    group by date(ts)
    union all
    select '1341' as rk, 'Stablecoins' as item, date(ts) as period, sum(case when account like '1341%' then amount end) as amount
    from entries
    group by date(ts)
    union all
    select '19' as rk, 'Others assets' as item, date(ts) as period, 
        -- Ideally exclude all detailled stuff above
        sum(case when account not like '11%' 
            and account like '14%'  
            then amount end) as amount
    from entries
    group by date(ts)
    union all
    select '2111' as rk, 'DSR' as item, date(ts) as period, -sum(case when account like '2111%' then amount end) as amount
    from entries
    group by date(ts)
    union all
    select '2112' as rk, 'DAI' as item, date(ts) as period, -sum(case when account like '2112%' then amount end) as amount
    from entries
    group by date(ts)
    union all
    select '3' as rk, 'Equity' as item, date(ts) as period, -sum(case when cast(account as varchar) like '3%' then amount end) as amount
    from entries
    group by date(ts)
),
balances as (
    select rk, item, period, sum(amount) over (partition by item order by period) as balance
    from items
)
select item, cast(period as timestamp) as period, balance, 
    balance/sum(case when balance > 0 then balance end) over (partition by period) as normalized
from balances
where period >= date'2020-07-01'
order by rk asc