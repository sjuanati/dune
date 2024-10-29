with
    users as (
        select distinct account as addr from gnosis_safe_gnosis.AccountFactory_evt_NewAccount
    ),
    tokens as (
        select ticker, contract_address
        from (values
            ('aGnoEURe', 0xedbc7449a9b594ca4e053d9737ec5dc4cbccbfb2), -- Nov-07-2023 09:08:10 AM UTC
            ('sDAI', 0xaf204776c7245bf4147c2612bf6e5972ee483701),     -- Sep-28-2023 11:21:40 AM UTC
            ('wstETH', 0x6c76971f98945ae98dd7d4dfca8711ebea946ea6)    -- Feb-07-2023 01:42:30 PM UTC
        ) as t(ticker, contract_address)
    ),
    -- prices of sDAI and wstETH in EUR. We assume aGnoEURe equals 1 EUR
    prices as (
        select
            t.contract_address,
            case
                when t.contract_address = 0xedbc7449a9b594ca4e053d9737ec5dc4cbccbfb2 then 1  -- aGnoEURe is 1 EUR
                else p.price / eure_usd.price  -- Convert other tokens to EUR
            end as eur_price,
            case
                when t.contract_address = 0xedbc7449a9b594ca4e053d9737ec5dc4cbccbfb2 then eure_usd.price  -- aGnoEURe in USD
                else p.price  -- Other tokens in USD
            end as usd_price
        from tokens t
        left join prices.usd_latest p on t.contract_address = p.contract_address and p.blockchain = 'gnosis'
        cross join (
            select price from prices.usd_latest
            where blockchain = 'gnosis' and contract_address = 0xcb444e90d8198415266c6a2724b7900fb12fc56e  -- eure
        ) as eure_usd
    ),
    balances_amount as (
        select
            contract_address,
            sum(
                case
                    when "from" = u.addr then -value
                    when "to" = u.addr then value
                    else 0
                end
            ) / 1e18 as amount
        from erc20_gnosis.evt_transfer tr
        inner join tokens to using (contract_address)
        left join users u on u.addr in (tr."from", tr."to")
        where tr.evt_block_date > date '2023-02-01'
        group by 1
    ),
    balances_price as (
        select
            contract_address, 
            t.ticker,
            p.eur_price,
            ba.amount,
            ba.amount * if(p.eur_price is null, 1, p.eur_price) as value_eur,
            ba.amount * if(p.eur_price is null, 1, p.usd_price) as value_usd
        from balances_amount ba
        inner join tokens t using (contract_address)
        left join prices p using (contract_address)
    ),
    balances_totals as (
        select
            sum(value_eur) as value_eur_total,
            sum(value_usd) as value_usd_total
        from balances_price
    )
    
select * from balances_price, balances_totals
