/*
-- @title: Accounting
-- @author: Steakhouse Financial
-- @description: Generates double-entry accounting, which serves as the basis for calculating the profit & loss and balance sheet
-- @notes: N/A
-- @version:
        1.0 - 2023-10-23 - Initial version
        2.0 - 2024-07-03 - Using accounting v2
        3.0 - 2024-10-28 - Add in SSR view
        4.0 - 2024-10-30 - Merge query 
        5.0 - 2025-05-05 - Update the SSR view
        6.0 - 2025-05-10 - Add SKY token
        7.0 - 2025-08-05 - Add token price for SKY
*/

with
    token_addresses as (
        select token, price_address as addr from query_3150055 -- Treasury ERC20s (ie: ENS, AAVE, COMP, stkAAVE)
        union all
        select 'MKR' as token, 0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2 as addr
        union all
        select 'ETH' as token, 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as addr
        union all
        select 'DAI' as token, 0x6b175474e89094c44da98b954eedeac495271d0f as addr
        union all
        select 'USDS' as token, 0xdc035d45d973e3ec169d2276ddab16f1e407384f as addr
        union all
        select 'SKY' as token, 0x56072C95FAA701256059aa122697B133aDEd9279 as addr
    ),
    token_prices as (
        select
            minute as ts,
            t.token,
            case when t.token IN ('DAI', 'USDS') then 1 else price end as price
        from prices.usd p
        inner join token_addresses t
            on p.contract_address = t.addr
        where blockchain = 'ethereum'
        and minute >= timestamp '2019-11-01'
        union all
        --ENS price history doesn't go back far enough, so manually inputting the first value from 2021-12-17 00:00
        select timestamp '2021-11-09 00:02' as ts, 'ENS' as token, 44.3 as price
        union all
        --SKY price history doesn't go back far enough, so manually inputting price of MKR / 24000 at '2024-09-17 12:00'
        select timestamp '2024-09-17 12:00' as ts, 'SKY' as token, 0.0635042 price
    ),
    coa as (
        select account_id, account_label from query_3689733 -- Sky - Chart of Accounts
    ),
    eth_prices as (
        select * from token_prices where token = 'ETH'
    ),
    -- mark-to-market filling for assets, liabilities and equity (create x9999 records for every treasury token)
    m2m_filling as (
        select
            p.ts,
            null as hash,
            coa.account_id,
            0 as value,
            p.token,
            coa.account_label as descriptor,
            null as ilk
        from token_prices p
        cross join coa
        where p.token not in ('MKR', 'ETH')
        and coa.account_id in (
            19999, -- Currency Translation to Presentation Token (Assets)
            29999, -- Currency Translation to Presentation Token (Liabilities)
            39999  -- Currency Translation to Presentation Token (Equity)
        )
    ),
    with_prices as (
        select
            account_id,
            substr(cast(account_id as varchar), 1, 1) as s_account_id, -- to facilitate str comparisons in next queries
            acc.ts,
            acc.hash,
            acc.value,
            acc.token,
            acc.descriptor,
            acc.ilk,
            acc.value * case when acc.token IN ('DAI', 'USDS') then 1 else p.price end as dai_value,
            acc.value * case when acc.token IN ('DAI', 'USDS') then 1 else p.price end / ep.price as eth_value,
            ep.price as eth_price
        from coa
        left join (
            select
                ts,
                hash,
                account_id,
                value,
                token,
                descriptor,
                ilk
            from dune.steakhouse.result_sky_accounting_revenues_opex_liquidations
            union all
            select
                ts,
                hash,
                account_id,
                value,
                token,
                descriptor,
                ilk
            from dune.steakhouse.result_sky_interest_accruals_dsr
            union all
            select * from m2m_filling
            UNION ALL
            SELECT * from dune.steakhouse.result_sky_accounting_ssr_sky_incentives
        ) acc -- accounting data
        using (account_id)
        left join token_prices p
            on date(acc.ts) = date(p.ts)
            and extract(hour from acc.ts) = extract(hour from p.ts)
            and extract(minute from acc.ts) = extract(minute from p.ts)
            and acc.token = p.token
        left join eth_prices ep
            on date(acc.ts) = date(ep.ts)
            and extract(hour from acc.ts) = extract(hour from ep.ts)
            and extract(minute from acc.ts) = extract(minute from ep.ts)
        where value is not null
    ),
    cumulative_sums as (
        select
            acc.*,
            sum(value) over (partition by s_account_id, acc.token order by acc.ts) as cumulative_ale_token_value,
            p.price * sum(value) over (partition by s_account_id, acc.token order by acc.ts)
            - sum(dai_value) over (partition by s_account_id, acc.token order by acc.ts) as dai_m2m,
            p.price/acc.eth_price * sum(value) over (partition by s_account_id, acc.token order by acc.ts)
            - sum(eth_value) over (partition by s_account_id, acc.token order by acc.ts) as eth_m2m
        from with_prices acc -- accounting data
        left join token_prices as p -- price data
            on acc.token = p.token
            and acc.ts = p.ts
    ),
    incremental_m2m as (
        select
            *,
            dai_m2m - coalesce(lag(dai_m2m) over (partition by s_account_id, token order by ts), 0) as incremental_dai_m2m,
            eth_m2m - coalesce(lag(eth_m2m) over (partition by s_account_id, token order by ts), 0) as incremental_eth_m2m
        from cumulative_sums
        where cumulative_ale_token_value > 0
        and substr(cast(account_id as varchar), -4) = '9999'
    ),
    final as (
        select
            account_id as code,
            ts,
            hash,
            value,
            token,
            descriptor,
            ilk,
            case when descriptor = 'MKR Vest Creates/Yanks' then 0 else dai_value end as dai_value,
            case when descriptor = 'MKR Vest Creates/Yanks' then 0 else eth_value end as eth_value,
            date(ts) as dt
        from with_prices
        where substr(cast(account_id as varchar), -4) <> '9999'
        union all
        select
            account_id as code,
            ts,
            hash,
            null as value,
            token,
            descriptor,
            ilk,
            incremental_dai_m2m as dai_value,
            incremental_eth_m2m as eth_value,
            date(ts) as dt
        from incremental_m2m
    )

-- test query
/*
select sum(value) as value, sum(coalesce(dai_value,0)) as dai_value, sum(coalesce(eth_value,0)) as eth_value
from final
where (coalesce(value, 0) <> 0 or dai_value <> 0 or eth_value <> 0)
and ts <= (select max(ts) + interval '59' second from eth_prices)
and ts < date '2024-05-21'
*/

select *
from final
where (coalesce(value, 0) <> 0 or dai_value <> 0 or eth_value <> 0)
and ts <= (select MAX(ts) + interval '59' second from eth_prices)
