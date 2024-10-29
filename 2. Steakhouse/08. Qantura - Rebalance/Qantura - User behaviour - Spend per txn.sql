with
    -- trades in cowswap where the trader is an account linked to a coordinator
    trades as (
        select
            date_trunc('week', t.block_date) as period,
            if(t.buy_token_address = 0xcb444e90d8198415266c6a2724b7900fb12fc56e, 'eur', 'gbp') as curr,
            coalesce(t.units_bought, 0) as amount
        from cow_protocol_gnosis.trades t
        inner join (select distinct account from gnosis_safe_gnosis.accountfactory_evt_newaccount) a
            on t.trader = a.account
        where date(t.block_date) > date('2024-09-01')
        and t.buy_token_address in (
            0xcb444e90d8198415266c6a2724b7900fb12fc56e, -- eure
            0x5cb9073902f2035222b9749f8fb0c9bfe5527108  -- gbpe
        )
    ),
    -- calculate approximate percentiles for each currency
    percentiles as (
        select
            period,
            curr,
            amount,
            approx_percentile(amount, 0.05) over (partition by period, curr) as tp5,  -- 5% of trades are <= this amount (percentile 5)
            approx_percentile(amount, 0.10) over (partition by period, curr) as tp10, -- 10% of trades are <= this amount (percentile 10)
            approx_percentile(amount, 0.50) over (partition by period, curr) as tp50, -- 50% of trades are <= this amount (median)
            approx_percentile(amount, 0.90) over (partition by period, curr) as tp90, -- 90% of trades are <= this amount (percentile 90)
            approx_percentile(amount, 0.99) over (partition by period, curr) as tp99  -- 99% of trades are <= this amount (percentile 99)
        from trades
    ),
    -- pivot percentiles per currency into columns
    percentiles_aggr as (
        select
            period,
            max(case when curr = 'eur' then tp5 end) as tp5_eur,
            max(case when curr = 'eur' then tp10 end) as tp10_eur,
            max(case when curr = 'eur' then tp50 end) as tp50_eur,
            max(case when curr = 'eur' then tp90 end) as tp90_eur,
            max(case when curr = 'eur' then tp99 end) as tp99_eur,
            max(case when curr = 'gbp' then tp5 end) as tp5_gbp,
            max(case when curr = 'gbp' then tp10 end) as tp10_gbp,
            max(case when curr = 'gbp' then tp50 end) as tp50_gbp,
            max(case when curr = 'gbp' then tp90 end) as tp90_gbp,
            max(case when curr = 'gbp' then tp99 end) as tp99_gbp,
            avg(case when curr = 'eur' and amount <= tp5 then amount end) as avg_tp5_eur,   -- average amount <= tp5
            avg(case when curr = 'eur' and amount <= tp10 then amount end) as avg_tp10_eur, -- average amount <= tp10
            avg(case when curr = 'eur' and amount <= tp50 then amount end) as avg_tp50_eur, -- average amount <= tp50
            avg(case when curr = 'eur' and amount <= tp90 then amount end) as avg_tp90_eur, -- average amount <= tp90
            avg(case when curr = 'eur' and amount <= tp99 then amount end) as avg_tp99_eur, -- average amount <= tp99
            avg(case when curr = 'gbp' and amount <= tp5 then amount end) as avg_tp5_gbp,
            avg(case when curr = 'gbp' and amount <= tp10 then amount end) as avg_tp10_gbp,
            avg(case when curr = 'gbp' and amount <= tp50 then amount end) as avg_tp50_gbp,
            avg(case when curr = 'gbp' and amount <= tp90 then amount end) as avg_tp90_gbp,
            avg(case when curr = 'gbp' and amount <= tp99 then amount end) as avg_tp99_gbp
        from percentiles
        group by period
    ),
    stats as (
        select
            period,
            max(if(curr='eur', amount, null)) as max_amount_eur,
            max(if(curr='gbp', amount, null)) as max_amount_gbp,
            min(if(curr='eur', amount, null)) as min_amount_eur,
            min(if(curr='gbp', amount, null)) as min_amount_gbp,
            avg(if(curr='eur', amount, null)) as avg_amount_eur,
            avg(if(curr='gbp', amount, null)) as avg_amount_gbp
        from trades
        group by period
    )

select *
from percentiles_aggr
inner join stats using(period)
where period < date_trunc('week', current_timestamp)
order by 1 desc
