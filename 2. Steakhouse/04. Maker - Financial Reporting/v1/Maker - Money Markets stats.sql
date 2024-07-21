-- deprecated
with dates as (
    select date
    from generate_series('2021-11-01'::date, current_date, '1 day') date
),
 d3m_addresses as (
    select distinct u as d3m_addresses
    from makermcd."VAT_call_frob"
    where replace(encode(i, 'escape'), '\000', '') like 'DIRECT-%'
),
fees_tx as (
  select distinct tx_hash
    from ethereum."logs"
    where contract_address = '\x9759a6ac90977b93b58547b4a71c78317f391a28'
        and topic1 = '\x3b4da69f00000000000000000000000000000000000000000000000000000000'
        and topic2 in (
            select  '\x000000000000000000000000'::bytea || d3m_addresses
            from d3m_addresses)
        and topic3 = '\x000000000000000000000000'::bytea || '\xa950524441892a31ebddf91d3ceefa04bf454466'::bytea -- Vow
),
frobs as (
    select call_block_time::date as date, replace(encode(i, 'escape'), '\000', ''), sum(dink/10^18) as amount 
    from makermcd."VAT_call_frob"
    where replace(encode(i, 'escape'), '\000', '') like 'DIRECT-%'
        and call_success
    group by call_block_time::date, 2
),
fees as (
    select "call_block_time"::date as date, sum(wad/10^18) as fees
    from makermcd."JOIN_DAI_call_join"
    where call_tx_hash in (
        select tx_hash 
        from fees_tx)
    --   and src in ('\x89b78cfa322f6c5de0abceecab66aee45393cc5a') -- List of PSM, to be cautious
    group by 1
),
group_by as (
    select date, 
        sum(greatest(amount, 0)) as inflow, 
        -sum(least(amount, 0)) as outflow, 
        sum(abs(amount)) as turnover, 
        sum(amount) as change,
        sum(fees) as fees
    from dates
    left join frobs using (date)
    left join fees using (date)
    group by 1
)
select *,
    sum(change) over (order by date asc) as d3m_balance,
    sum(turnover) over (order by date asc) as lifetime_turnover,
    sum(fees) over (order by date asc) as lifetime_fees
from dates
left outer join group_by using (date)
order by date desc