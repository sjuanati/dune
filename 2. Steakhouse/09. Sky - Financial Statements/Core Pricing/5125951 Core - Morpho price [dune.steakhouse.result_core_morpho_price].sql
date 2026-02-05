/*
-- @title: Core - Morpho price
-- @description: Handles prices based on shares through deposit & withdrawal events in morpho
-- @author: Steakhouse Financial
-- @notes: only morpho vaults included in the <token_info_inclusions dataset> will be handled:
--         https://docs.google.com/spreadsheets/d/12_T-9xpMBNKionRbYsStFTHWmTzX_VFXLBbu6nU6F30/edit?gid=0#gid=0
-- @version:
    - 1.0 - 2025-05-13 - Initial version
    - 2.0 - 2025-05-26 - Replaced prices.usd_daily & dex.prices by prices.day + added stablecoins join
*/

    ---------------------------------------------
    -- Morpho vault tokens
    -- @dev: decimals can differ between assets and shares; use field decimals from the inclusions dataset
    ---------------------------------------------
with    
    morpho as (
        select
            date(dt) as dt,
            i.blockchain,
            i.symbol,
            i.token_address,
            i.underlying_address as price_address,
            max_by(m.assets * pow(10, -i.underlying_decimals), m.dt) as assets,
            max_by(m.shares * pow(10, -i.decimals), m.dt) as shares
        from (
            select * from dune.steakhouse.dataset_token_info_inclusions where "type" = 'morpho'
        ) i
        join (
            select evt_block_time as dt, contract_address, assets, shares
            from metamorpho_vaults_ethereum.metamorpho_evt_deposit
            where shares > 10
            union all
            select evt_block_time as dt, contract_address, assets, shares
            from metamorpho_vaults_ethereum.metamorpho_evt_withdraw
            where shares > 10
            union all
            select evt_block_time as dt, contract_address, assets, shares
            from metamorpho_vaults_ethereum.metamorphov1_1_evt_deposit
            where shares > 10
            union all
            select evt_block_time as dt, contract_address, assets, shares
            from metamorpho_vaults_ethereum.metamorphov1_1_evt_withdraw
            where shares > 10
        ) m
            on m.contract_address = i.token_address
        group by 1, 2, 3, 4, 5
    ),
    default_pricing as (
        select
            m.dt,
            m.blockchain,
            m.symbol,
            m.token_address,
            m.price_address,
            m.assets / cast(m.shares as double) as share_price,
            min_by(p.price, source) * (m.assets / cast(m.shares as double)) as price_usd
        from morpho m
        join prices.day p
            on m.dt = date(p."timestamp")
            and m.blockchain = p.blockchain
            and m.price_address = p.contract_address
        group by 1,2,3,4,5,6
    ),
    -- in case the vault's underlying token is a stablecoin not included in dune's pricing, we use 1 by default (eg: USDR)
    stablecoin_pricing as (
        select
            m.dt,
            m.blockchain,
            m.symbol,
            m.token_address,
            m.price_address,
            m.assets / cast(m.shares as double) as share_price,
            1 * (m.assets / cast(m.shares as double)) as price_usd
        from morpho m
        join dune.steakhouse.dataset_token_info_inclusions i
            on m.blockchain = i.blockchain
            and m.price_address = i.token_address
        where i."type" = 'usd-stablecoin'
    ),    
    -- eg: vaults using wUSDL
    alternative_pricing as (
        select
            m.dt,
            m.blockchain,
            m.symbol,
            m.token_address,
            m.price_address,
            max_by(m.assets / cast(m.shares as double), p.dt) as share_price,
            max_by(p.price_usd * (m.assets / cast(m.shares as double)), p.dt) as price_usd
        from morpho m
        join dune.steakhouse.result_token_alternative_price p
            on m.dt = p.dt
            and m.blockchain = p.blockchain
            and m.price_address = p.token_address
        group by 1,2,3,4,5
    ),
    pricing as (
        select
            dt,
            blockchain,
            m.symbol,
            token_address,
            m.price_address, 
            df.price_usd as default_price_usd,
            coalesce(al.share_price, df.share_price, s.share_price) as share_price,
            coalesce(al.price_usd, df.price_usd, s.price_usd) as price_usd
        from morpho m
        left join default_pricing df using (dt, blockchain, token_address)
        left join alternative_pricing al using (dt, blockchain, token_address)
        left join stablecoin_pricing s using (dt, blockchain, token_address)
    )

select * from pricing where price_usd is not null -- remove the current day if no prices yet