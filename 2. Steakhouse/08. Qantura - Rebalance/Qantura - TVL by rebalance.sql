with
    -- coordinators that have a recipient; therefore, balancing stuff
    balance_coordinators as (
        select coordinator from gnosis_safe_gnosis.CoordinatorFactory_evt_NewCoordinator where recipient != 0x0000000000000000000000000000000000000000
        union
        select contract_address from gnosis_chain_gnosis.Coordinator_evt_NewRecipient
    ),
    -- all coordinators
    coordinators as (
        select
            nc.coordinator,
            if(bc.coordinator is not null, true, false) as is_rebalancer
        from gnosis_safe_gnosis.CoordinatorFactory_evt_NewCoordinator nc
        left join balance_coordinators bc on nc.coordinator = bc.coordinator
    ),
    users as (
        select
            account as addr,
            is_rebalancer
        from gnosis_safe_gnosis.AccountFactory_evt_NewAccount a
        inner join coordinators bc using(coordinator)
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
            p.contract_address,
            p.price / e.eure_usd_price as eur_price
        from prices.usd_latest p
        cross join (
            select price as eure_usd_price from prices.usd_latest
            where blockchain = 'gnosis' and contract_address = 0xcb444e90d8198415266c6a2724b7900fb12fc56e  -- eure
        ) e
        where p.blockchain = 'gnosis'
        and p.contract_address in (
            0xaf204776c7245bf4147c2612bf6e5972ee483701,  --sdai
            0x6c76971f98945ae98dd7d4dfca8711ebea946ea6 -- wsteth
        )
    ),
    balances_amount as (
        select
            contract_address,
            u.is_rebalancer,
            sum(
                case
                    when "from" = u.addr then -value
                    when "to" = u.addr then value
                    else 0
                end
            ) / 1e18 as amount
        from erc20_gnosis.evt_transfer tr
        inner join tokens to using (contract_address)
        inner join users u on u.addr in (tr."from", tr."to") -- todo: should be inner?
        where tr.evt_block_date > date '2023-02-01'
        group by 1, 2
    ),
    balances_eur as (
        select
            contract_address,
            is_rebalancer,
            t.ticker,
            p.eur_price,
            ba.amount,
            ba.amount * if(p.eur_price is null, 1, p.eur_price) as value
        from balances_amount ba
        inner join tokens t using (contract_address)
        left join prices p using (contract_address)
    ),
    totals as (
        select
            if(is_rebalancer = true, 'rebalance', 'non rebalance') as is_rebalancer,
            sum(value) as value
        from balances_eur
        group by 1
    )

select * from totals
