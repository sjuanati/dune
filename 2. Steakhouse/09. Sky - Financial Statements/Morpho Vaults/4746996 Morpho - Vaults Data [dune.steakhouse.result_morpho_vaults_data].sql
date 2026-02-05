/*
@title: Morpho V2 - Vaults Data
@description: Provides an overview of Morpho vaults
@author: Steakhouse Financial
@references:
    1) https://morpho.blockanalitica.com/metamorpho/
    2) https://legacy.morpho.org/vault?vault={VAULT_ADDRESS_HERE}&network=mainnet
    3) V1 query: https://dune.com/queries/3959703
@definitions:
    - Assets: amount of the underlying token (asset) that the contract holds (ie: USDC)
    - Shares: tokens issued by the vault to represent a user’s proportional ownership of the underlying assets
    - Share Price: conversion rate between shares & assets, increases over time (as interest accrues)
                   how many underlying tokens (eg: USDT) correspond to 1 share
@notes: naming convention changes V1 vs. V2
    V1                     V2
    metamorpho             vault_address
    name                   vault_name
    market_symbol          vault_symbol
    coll_decimals          token_decimals
    symbol, coll_symbol    token_symbol
    asset                  token_address
@version:
    - 1.0 - 2025-02-26 - Initial version
    - 2.0 - 2025-04-13 - Refactor queries in V2 format
                        -> Modify the Aggregate query variables
                        -> Remove the Metamorpho Calcs CTE
    - 3.0 - 2025-04-21 - Set a minimal precision for assets and shares
    - 4.0 - 2025-04-26 - Add liquidity and liquidity_ratio
    - 5.0 - 2025-04-30 - Removed update_assets CTE and its link in the vault_accrued_fees CTE
    - 6.0 - 2025-05-20 - Add feeShares into the total supply shares
    - 7.0 - 2025-06-19 - Coalesce accounting_price_usd first
    - 8.0 - 2025-07-15 - Use Mat view of Morpho vaults_data query
    - 9.0 - 2025-07-17 - Add Markets Supply Rate in Final Output
    - 9.1 - 2025-09-28 - interest fees shares where actually in usd and not in shares
    - 9.2 - 2025-10-09 - Update Morpho vaults precisions to (assets and shares > 300 filter out small txs)
    - 10.0 - 2026-01-06 - Update the interpolation for the end stage
*/

with
    -- get all metamorpho vaults created from factory contracts
    vault_data as (
        select
            creation_date as creation_ts,
            DATE(creation_date) AS creation_dt,
            blockchain,
            token_address,
            vault_address,
            vault_name,
            vault_symbol,
            token_symbol,
            token_decimals,
            18 as market_decimals,
            curator_name as curator,
            version
        from dune.steakhouse.result_morpho_vaults
        -- query_4835775 -- Morpho - Vaults
    ),
    -- get the vault token (that users deposit/withdraw; eg: USDC) and the vault's create date to generate time series
    vault_tokens as (
        select blockchain, vault_address, token_address, min(creation_dt) as start_time from vault_data group by 1, 2, 3
    ),
    -- generate a time sequence starting from the vault creation date
    vault_series as (
        select
            t.dt,
            v.blockchain,
            v.vault_address,
            v.token_address
        from vault_data v
        cross join unnest(sequence(v.creation_dt, current_date, interval '1' day)) as t(dt)
    ),
    -- all metamorpho supply events
    supply_events as (
        select evt_block_time, chain, contract_address, shares, assets, owner, 'deposit' as side
        from metamorpho_vaults_multichain.metamorpho_evt_deposit
        union all
        -- select evt_block_time, chain, contract_address, shares, assets, owner, 'deposit' as side
        -- from metamorpho_vaults_multichain.metamorphov1_1_evt_deposit
        -- where chain not in ('monad')
        -- union all
        select evt_block_time, chain, contract_address, shares, assets, owner, 'withdraw' as side
        from metamorpho_vaults_multichain.metamorpho_evt_withdraw
        -- union all
        -- select evt_block_time, chain, contract_address, shares, assets, owner, 'withdraw' as side
        -- from metamorpho_vaults_multichain.metamorphov1_1_evt_withdraw
        -- where chain not in ('monad')
        union all
        select evt_block_time, 'arbitrum' as chain, contract_address, shares, assets, owner, 'withdraw' as side
        from metamorpho_vaults_arbitrum.metamorphov1_1_evt_withdraw
        union all
        select evt_block_time, 'base' as chain, contract_address, shares, assets, owner, 'withdraw' as side
        from metamorpho_vaults_base.metamorphov1_1_evt_withdraw
        union all
        select evt_block_time, 'celo' as chain, contract_address, shares, assets, owner, 'withdraw' as side
        from metamorpho_vaults_celo.metamorphov1_1_evt_withdraw
        union all
        select evt_block_time, 'corn' as chain, contract_address, shares, assets, owner, 'withdraw' as side
        from metamorpho_vaults_corn.metamorphov1_1_evt_withdraw
        union all
        select evt_block_time, 'ethereum' as chain, contract_address, shares, assets, owner, 'withdraw' as side
        from metamorpho_vaults_ethereum.metamorphov1_1_evt_withdraw
        union all
        select evt_block_time, 'hemi' as chain, contract_address, shares, assets, owner, 'withdraw' as side
        from metamorpho_vaults_hemi.metamorphov1_1_evt_withdraw
        union all
        select evt_block_time, 'hyperevm' as chain, contract_address, shares, assets, owner, 'withdraw' as side
        from metamorpho_vaults_hyperevm.metamorphov1_1_evt_withdraw
        union all
        select evt_block_time, 'katana' as chain, contract_address, shares, assets, owner, 'withdraw' as side
        from metamorpho_vaults_katana.metamorphov1_1_evt_withdraw
        union all
        select evt_block_time, 'monad' as chain, contract_address, shares, assets, owner, 'withdraw' as side
        from metamorpho_vaults_monad.metamorphov1_1_evt_withdraw
        union all
        select evt_block_time, 'polygon' as chain, contract_address, shares, assets, owner, 'withdraw' as side
        from metamorpho_vaults_polygon.metamorphov1_1_evt_withdraw
        union all
        select evt_block_time, 'optimism' as chain, contract_address, shares, assets, owner, 'withdraw' as side
        from metamorpho_vaults_optimism.metamorphov1_1_evt_withdraw
        union all
        select evt_block_time, 'plume' as chain, contract_address, shares, assets, owner, 'withdraw' as side
        from metamorpho_vaults_plume.metamorphov1_1_evt_withdraw
        union all
        select evt_block_time, 'sei' as chain, contract_address, shares, assets, owner, 'withdraw' as side
        from metamorpho_vaults_sei.metamorphov1_1_evt_withdraw
        union all
        select evt_block_time, 'sonic' as chain, contract_address, shares, assets, owner, 'withdraw' as side
        from metamorpho_vaults_sonic.metamorphov1_1_evt_withdraw
        union all
        select evt_block_time, 'tac' as chain, contract_address, shares, assets, owner, 'withdraw' as side
        from metamorpho_vaults_tac.metamorphov1_1_evt_withdraw
        union all
        select evt_block_time, 'unichain' as chain, contract_address, shares, assets, owner, 'withdraw' as side
        from metamorpho_vaults_unichain.metamorphov1_1_evt_withdraw
        union all
        select evt_block_time, 'worldchain' as chain, contract_address, shares, assets, owner, 'withdraw' as side
        from metamorpho_vaults_worldchain.metamorphov1_1_evt_withdraw
        union all
        /*DEPOSIT*/
        select evt_block_time, 'arbitrum' as chain, contract_address, shares, assets, owner, 'deposit' as side
        from metamorpho_vaults_arbitrum.metamorphov1_1_evt_deposit
        union all
        select evt_block_time, 'base' as chain, contract_address, shares, assets, owner, 'deposit' as side
        from metamorpho_vaults_base.metamorphov1_1_evt_deposit
        union all
        select evt_block_time, 'celo' as chain, contract_address, shares, assets, owner, 'deposit' as side
        from metamorpho_vaults_celo.metamorphov1_1_evt_deposit
        union all
        select evt_block_time, 'corn' as chain, contract_address, shares, assets, owner, 'deposit' as side
        from metamorpho_vaults_corn.metamorphov1_1_evt_deposit
        union all
        select evt_block_time, 'ethereum' as chain, contract_address, shares, assets, owner, 'deposit' as side
        from metamorpho_vaults_ethereum.metamorphov1_1_evt_deposit
        union all
        select evt_block_time, 'hemi' as chain, contract_address, shares, assets, owner, 'deposit' as side
        from metamorpho_vaults_hemi.metamorphov1_1_evt_deposit
        union all
        select evt_block_time, 'hyperevm' as chain, contract_address, shares, assets, owner, 'deposit' as side
        from metamorpho_vaults_hyperevm.metamorphov1_1_evt_deposit
        union all
        select evt_block_time, 'katana' as chain, contract_address, shares, assets, owner, 'deposit' as side
        from metamorpho_vaults_katana.metamorphov1_1_evt_deposit
        union all
        select evt_block_time, 'monad' as chain, contract_address, shares, assets, owner, 'deposit' as side
        from metamorpho_vaults_monad.metamorphov1_1_evt_deposit
        union all
        select evt_block_time, 'polygon' as chain, contract_address, shares, assets, owner, 'deposit' as side
        from metamorpho_vaults_polygon.metamorphov1_1_evt_deposit
        union all
        select evt_block_time, 'optimism' as chain, contract_address, shares, assets, owner, 'deposit' as side
        from metamorpho_vaults_optimism.metamorphov1_1_evt_deposit
        union all
        select evt_block_time, 'plume' as chain, contract_address, shares, assets, owner, 'deposit' as side
        from metamorpho_vaults_plume.metamorphov1_1_evt_deposit
        union all
        select evt_block_time, 'sei' as chain, contract_address, shares, assets, owner, 'deposit' as side
        from metamorpho_vaults_sei.metamorphov1_1_evt_deposit
        union all
        select evt_block_time, 'sonic' as chain, contract_address, shares, assets, owner, 'deposit' as side
        from metamorpho_vaults_sonic.metamorphov1_1_evt_deposit
        union all
        select evt_block_time, 'tac' as chain, contract_address, shares, assets, owner, 'deposit' as side
        from metamorpho_vaults_tac.metamorphov1_1_evt_deposit
        union all
        select evt_block_time, 'unichain' as chain, contract_address, shares, assets, owner, 'deposit' as side
        from metamorpho_vaults_unichain.metamorphov1_1_evt_deposit
        union all
        select evt_block_time, 'worldchain' as chain, contract_address, shares, assets, owner, 'deposit' as side
        from metamorpho_vaults_worldchain.metamorphov1_1_evt_deposit
    )
    -- get all deposits & withdraws from the vaults
    , vault_supply as (
        select
            evt_block_time as ts,
            date(d.evt_block_time) as dt,
            d.chain as blockchain,
            v.vault_address,
            d.assets as raw_assets,
            v.token_decimals,
            if(side = 'deposit', 1, -1) * (d.shares / power(10, v.market_decimals)) as supply_shares,
            if(side = 'deposit', 1, -1) * (d.assets / power(10, v.token_decimals)) as supply_amount
        from supply_events d
        join vault_data v
            on d.contract_address = v.vault_address
            and d.chain = v.blockchain
    ),
    shares_pricing as (
        SELECT ts
            , 'event' as row_type
            , blockchain
            , vault_address
            , max(round(supply_amount/supply_shares, 15)) as share_price
            , max(to_unixtime(ts)) as share_unix_ts
        FROM vault_supply
        WHERE raw_assets >= 1e3
        GROUP BY 1, 3, 4
        UNION ALL
        -- At creation markets all supplies/borrows start at share price 1.0. 
        SELECT creation_ts
            , 'event' as row_type
            , blockchain
            , vault_address
            , 1.0 as share_price
            , to_unixtime(creation_ts) as share_unix_ts
        FROM vault_data
        UNION ALL
        SELECT dt
            , 'date' as row_type
            , blockchain
            , vault_address
            , NULL as share_price
            , NULL as share_unix_ts
        FROM vault_series
    )
    , events_with_periods as (
        SELECT ts
            , to_unixtime(ts) as unix_ts
            , vault_address
            , blockchain
            , row_type
            -- Share Price
            , FIRST_VALUE(share_price) IGNORE NULLS OVER (PARTITION BY vault_address, blockchain ORDER BY TS DESC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) as previous_price
            , NTH_VALUE(share_price, 2) IGNORE NULLS OVER (PARTITION BY vault_address, blockchain ORDER BY TS DESC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) as previous_previous_price
            , FIRST_VALUE(share_price) IGNORE NULLS OVER (PARTITION BY vault_address, blockchain ORDER BY TS ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) as next_price
            , NTH_VALUE(share_price, 2) IGNORE NULLS OVER (PARTITION BY vault_address, blockchain ORDER BY TS ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) as next_next_price

            -- Unix Ts
            , FIRST_VALUE(share_unix_ts) IGNORE NULLS OVER (PARTITION BY vault_address, blockchain ORDER BY TS DESC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) as previous_ts
            , NTH_VALUE(share_unix_ts, 2) IGNORE NULLS OVER (PARTITION BY vault_address, blockchain ORDER BY TS DESC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) as previous_previous_ts
            , FIRST_VALUE(share_unix_ts) IGNORE NULLS OVER (PARTITION BY vault_address, blockchain ORDER BY TS ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) as next_ts
            , NTH_VALUE(share_unix_ts, 2) IGNORE NULLS OVER (PARTITION BY vault_address, blockchain ORDER BY TS ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) as next_next_ts 
        FROM shares_pricing
    )
    , shares_pricing_series as (
        SELECT ts
            , vault_address
            , blockchain
            , row_type
            , CASE 
                -- MIDDLE STAGE
                WHEN previous_price IS NOT NULL AND next_price IS NOT NULL
                    THEN previous_price + (next_price - previous_price)/(next_ts - previous_ts) * (unix_ts - previous_ts)
                
                -- BEGINNING STAGE
                WHEN next_price IS NOT NULL AND next_next_price IS NOT NULL
                    THEN GREATEST(1, next_price - (next_next_price - next_price)/(next_next_ts - next_ts) * (next_next_ts - unix_ts))

                 -- END STAGE Exclude one event scenarios
                WHEN previous_price IS NOT NULL AND previous_previous_price IS NOT NULL AND previous_ts != previous_previous_ts
                    THEN previous_price + (previous_price - previous_previous_price)/(previous_ts - previous_previous_ts) * (unix_ts - previous_ts)
                ELSE COALESCE(previous_price, next_price)
            END as share_price
        from events_with_periods
    )
    , share_price_daily as (
        select ts as dt -- always midnight date
            , blockchain
            , vault_address
            , share_price
            , LAG(share_price) IGNORE NULLS OVER (PARTITION BY blockchain, vault_address ORDER BY ts) as previous_share_price
        from shares_pricing_series
        WHERE row_type = 'date'
    )
    -- get the USD price for the vault tokens
    , token_prices as (
        select
            p.dt,
            blockchain,
            t.vault_address,
            token_address,
            coalesce(p.accounting_price_usd, p.price_usd) as price_usd
        from vault_tokens t
        join dune.steakhouse.result_token_price p using (blockchain, token_address)
        where p.dt >= t.start_time
    ),
    -- calculate the share price of the supplied amounts into the vault, based on assets/shares ratio
    share_prices as (
        SELECT dt
            , blockchain
            , vault_address
            , share_price
            , previous_share_price
            , share_price * COALESCE(price_usd, LAG(price_usd) OVER (PARTITION BY blockchain, vault_address ORDER BY dt)) as share_price_usd 
        FROM share_price_daily
            LEFT JOIN token_prices USING (dt, blockchain, vault_address)
    ),
    -- get accrued interest (fees earned) from vaults (emitted only with Deposits/Withdraws) and the new total supply (after interest accrual)
    -- we retrieve the Maker Direct Hub for the fees when the vault is spDAI (feeShares is 0)
    accrued_interests as (
        select
            i.evt_block_time,
            i.chain as blockchain,
            i.contract_address as vault_address,
            i.evt_index,
            i.evt_block_number,
            coalesce(i.feeShares, f.amt) * 1e-18 as fees
        from (
            select * from metamorpho_vaults_multichain.metamorpho_evt_accrueinterest
            union all
            select * from metamorpho_vaults_multichain.metamorphov1_1_evt_accrueinterest
        ) i
        join vault_data v
            on i.chain = v.blockchain
            and i.contract_address = v.vault_address
        -- Specific for D3M Fees Outputted only in D3MHub
        left join maker_ethereum.D3MHub_evt_Fees f
            on i.evt_tx_hash = f.evt_tx_hash
            and i.evt_block_time = f.evt_block_time
            and i.chain = 'ethereum'
            and i.contract_address = 0x73e65dbd630f90604062f6e02fab9138e713edd9 -- Spark DAI Vault
            and f.evt_block_number >= 16069947 -- Nov-28-2022 05:21:35 PM +UTC
            and from_utf8(bytearray_rtrim(ilk)) = 'DIRECT-SPARK-MORPHO-DAI'
    ),
    -- sum vault's accrued fees
    vault_accrued_fees as (
        select
            date(i.evt_block_time) as dt,
            i.blockchain,
            i.vault_address,
            sum(coalesce(i.fees, 0)) as fees
        from accrued_interests i
        where i.fees != 0
        group by 1, 2, 3
    ),
    -- get utilization rate and supply rate at aggregated at vault's markets level
    vault_markets_aggregation as (
        select
            dt,
            blockchain,
            vault_address,
            utilization_ratio,
            liquidity,
            collaterals,
            supply_rate
        from dune.steakhouse.result_metamorpho_utilisation
    ),
    -- gets the fee settings per vault
    vault_performance_fees as (
        select
            date(evt_block_time) as dt,
            chain as blockchain,
            contract_address as vault_address,
            max_by(newFee * 1e-18, evt_block_time) as performance_fee
        from (
            select evt_block_time, chain, contract_address, newFee from metamorpho_vaults_multichain.metamorpho_evt_setfee
            union all
            select evt_block_time, chain, contract_address, newFee from metamorpho_vaults_multichain.metamorphov1_1_evt_setfee
        )
        group by 1, 2, 3
    ),
    -- calculate cumulative performance fees and supply rates, and join with utilization rate and accrued fees previously retrieved
    vault_metrics as (
        select dt,
            blockchain,
            vault_address,
            a.collaterals,
            coalesce(af.fees, 0) as fees,
            SUM(coalesce(af.fees, 0)) OVER (partition by blockchain, vault_address ORDER BY dt) as total_fees,
            COALESCE(a.utilization_ratio, last_value(a.utilization_ratio) ignore nulls over (partition by blockchain, vault_address order by dt rows between unbounded preceding and current row)) as utilization_rate,
            COALESCE(a.liquidity, last_value(a.liquidity) ignore nulls over (partition by blockchain, vault_address order by dt rows between unbounded preceding and current row)) as liquidity,
            COALESCE(a.supply_rate, last_value(a.supply_rate) ignore nulls over (partition by blockchain, vault_address order by dt rows between unbounded preceding and current row)) as supply_rate,
            coalesce(coalesce(pf.performance_fee, last_value(pf.performance_fee) ignore nulls over (partition by blockchain, vault_address order by dt rows between unbounded preceding and current row)), 0) as performance_fee
        from vault_series s
        left join vault_accrued_fees af using (dt, blockchain, vault_address)
        left join vault_markets_aggregation a using (dt, blockchain, vault_address)
        left join vault_performance_fees pf using (dt, blockchain, vault_address)
    )
    ,
    -- calculate cumulative supply shares & amounts
    supply_balance_daily as (
        select dt,
            blockchain,
            vault_address,
            sum(sum(coalesce(su.supply_shares, 0))) over (partition by blockchain, vault_address order by dt) as supply_total_shares,
            sum(sum(coalesce(su.supply_amount, 0))) over (partition by blockchain, vault_address order by dt) as supply_total_amount,
            sum(su.supply_shares) as supply_delta_shares -- supply_change
        from vault_series se
        left join vault_supply su using (dt, blockchain, vault_address)
        group by 1, 2, 3
    ),
    metamorpho_daily as (
        SELECT dt
            , blockchain
            , vault_address
            , performance_fee

            -- Share Price
            , share_price_usd
            , share_price
            , previous_share_price

            , CASE WHEN fees + supply_delta_shares != 0 THEN fees + supply_delta_shares END AS supply_delta_shares
            , (total_fees + supply_total_shares) as supply_total_shares
            , (total_fees + supply_total_shares) * share_price as supply_total_amount

            -- USD Values
            , (fees + supply_delta_shares) * share_price_usd as delta_supply_usd
            , (total_fees + supply_total_shares) * share_price_usd as supply_usd
            
            -- Vault Metrics
            , vm.utilization_rate
            , vm.liquidity
            , vm.collaterals
            , vm.supply_rate as markets_supply_rate  -- Markets APY (Derived from underlying markets) 
            , (1 - vm.performance_fee) * ((share_price / previous_share_price) - 1) * 365 as supply_rate  -- Native APY Rate after fee
            , CASE WHEN fees != 0 THEN fees END as fees
        FROM supply_balance_daily as sbd
            JOIN share_prices as sp USING (dt, blockchain, vault_address)
            JOIN vault_metrics as vm USING (dt, blockchain, vault_address)
    )
--     -- final calculations using projections when the price share is not up-to-date
    , metamorpho_final as (
        SELECT dt
            , blockchain
            , vault_address
            , performance_fee

            , token_symbol
            , vault_name
            , vault_symbol
            , 'metamorpho' as platform
            , version
            
            , COALESCE(collaterals, ARRAY[token_symbol]) as collaterals
            , curator

            -- USD Units
            , delta_supply_usd as delta_supply_usd
            , CAST(NULL AS DOUBLE) as delta_borrow_usd
            , supply_usd as supply_usd -- [Total Market Size in USD]
            , CAST(NULL AS DOUBLE) as borrow_usd
            , (supply_usd) as total_usd -- [Total Market Size in USD]
            , (fees * share_price_usd) as interest_paid_usd 

            -- Shares Units
            , supply_delta_shares as supply_delta_shares
            , supply_total_shares as supply_shares
            , fees as interest_paid_shares

            -- Token Units
            , supply_total_amount as supply_amount

            , supply_rate
            , markets_supply_rate
            , utilization_rate
            , liquidity
            , case when supply_total_amount =  0 then 0 else liquidity / supply_total_amount end as liquidity_ratio
            , share_price AS supply_index

            , share_price -- expressed in underlying tokens
            , share_price_usd
        FROM metamorpho_daily
            JOIN vault_data USING (vault_address, blockchain)
    )

select * from metamorpho_final