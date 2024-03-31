with
    entries as (
        select 
            cast(account as varchar) as account, 
            amount, 
            date_trunc('month', ts) as period
        from dune.steakhouse.result_ens_accounting_main
        where ts >= date_trunc('month', current_date) - interval '13' month
          and ts < date_trunc('month', current_date)
    ),
    cash_entries as (
        select
            period,
            coalesce(sum(case when account like '322%' then amount end), 0) as operating_expenses,
            coalesce(sum(case when account like '312%' then amount end), 0) as issued_as_payment,
            abs(coalesce(sum(case when account like '322%' then amount end), 0) + coalesce(sum(case when account like '312%' then amount end), 0)) as cash_burn
        from entries
        group by period
    )
    
select
    c1.period,
    c1.operating_expenses,
    c1.issued_as_payment,
    c1.cash_burn,
    sum(c2.cash_burn * (13 - (extract(month from c1.period) - extract(month from c2.period) + 12 * (extract(year from c1.period) - extract(year from c2.period))))) / 
    sum(13 - (extract(month from c1.period) - extract(month from c2.period) + 12 * (extract(year from c1.period) - extract(year from c2.period)))) as normalized_cash_burn
from cash_entries c1
join cash_entries c2 on c2.period between date_trunc('month', c1.period) - interval '12' month and c1.period
group by c1.period, c1.operating_expenses, c1.issued_as_payment, c1.cash_burn
order by c1.period desc;
