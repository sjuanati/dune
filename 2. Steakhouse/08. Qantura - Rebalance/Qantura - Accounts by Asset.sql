with
    users as (
        select account from gnosis_safe_gnosis.AccountFactory_evt_NewAccount
    ),
    tokens as (
        select ticker, contract_address
        from (values
            ('aGnoEURe', 0xedbc7449a9b594ca4e053d9737ec5dc4cbccbfb2), -- Nov-07-2023 09:08:10 AM UTC
            ('sDAI', 0xaf204776c7245bf4147c2612bf6e5972ee483701),     -- Sep-28-2023 11:21:40 AM UTC
            ('wstETH', 0x6c76971f98945ae98dd7d4dfca8711ebea946ea6)    -- Feb-07-2023 01:42:30 PM UTC
        ) as t(ticker, contract_address)
    ),
    txns as (
        select
            to.ticker,
            u.account
        from erc20_gnosis.evt_transfer tr
        inner join tokens to using (contract_address)
        inner join users u on u.account in (tr."from", tr."to")
        where tr.evt_block_date > date '2023-02-01'
    ),
    totals as (
        select
            ticker,
            count(distinct account) as account
        from txns
        group by 1
    ),
    totals2 as (
        select distinct
            ticker,
            account
        from txns
    )

select * from totals