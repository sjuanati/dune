-- Sparklend Markets identifies all relevant markets and the metrics associated with it.
-- References:
-- 1) https://defillama.com/protocol/spark
-- 2) https://app.spark.fi/markets/
-- Changelog:
-- 2024-08-26: Partitioning by market to fix supply and borrow indexes and rates.
--              -> Fix Utilization Rate for small markets like WBTC
-- 2024-08-29: Prevented duplication for $USD values of supply/borrows.
--              -> Include token amounts for supplies and borrows.
-- 2024-10-21: Fix sDAI duplication.
-- 2024-10-23: Add in sUSDs, weETH, cbBTC Market.
-- 2024-11-23: Track supply and borrows in terms of the atoken values (which index the token amounts)
-- 2025-03-21: Fix spToken balances for calculating final values (include balance increase)
--             -> Refactor code
-- 2025-04-22 - Add in suffix for _usd for final columns
-- 2025-07-01 - Added USDS, LBTC, sUSDS, tBTC, ezETH atoken
-- 2025-08-27 - Add vdToken and aToken in final result
--             -> Add PYUSD
-- 2025-12-03 - Optimise Spark Protocol Markets Query
with days AS ( 
    SELECT day
    FROM unnest(sequence(
        timestamp'2023-05-01 00:00:00', 
        CAST(NOW() as timestamp),
        interval '1' day)
    ) as s(day)
)
, tokens as (
    SELECT all_tokens.symbol
        , all_tokens.blockchain
        , decimals as price_decimals
        , asset
        , atoken
        , stableDebtToken
        , variableDebtToken
        , evt_block_number as creation_block
        , evt_block_time as creation_time
        , evt_tx_hash
    FROM spark_protocol_ethereum.PoolConfigurator_evt_ReserveInitialized as reserve
        JOIN dune.steakhouse.result_token_info as all_tokens
            on all_tokens.token_address = asset
            and all_tokens.blockchain = 'ethereum'
)
, series_data as (
    SELECT days.day, tokens.blockchain, tokens.asset, tokens.aToken, variableDebtToken as vdToken, tokens.symbol
    FROM tokens, days
)
, supply_minted_amount as (

    SELECT day, symbol, atoken, blockchain, SUM(minted) as minted
    FROM (
        SELECT date_trunc('day', evt_block_time) as day
            , symbol, tokens.atoken
            , chain as blockchain
            , ((CAST(value AS INT256) - CAST(balanceIncrease AS INT256)) * POWER(10, -price_decimals))/(index * 1e-27) as minted
        FROM spark_protocol_multichain.atoken_evt_mint as evm_logs
            JOIN tokens on evm_logs.contract_address = tokens.atoken
        and chain = 'ethereum'
    )
    GROUP BY 1, 2, 3, 4
)
, supply_burn_amount as (

    SELECT day, symbol, atoken, blockchain, SUM(burnt) as burnt
    FROM (
        SELECT date_trunc('day', evt_block_time) as day
            , symbol, tokens.atoken
            , chain as blockchain
            , ((CAST(value AS INT256) + CAST(balanceIncrease AS INT256)) * POWER(10, -price_decimals))/(index * 1e-27) as burnt
        FROM spark_protocol_multichain.atoken_evt_burn as evm_logs
            JOIN tokens on evm_logs.contract_address = tokens.atoken
        -- Burns
        WHERE evm_logs.evt_block_number >= tokens.creation_block
        and chain = 'ethereum'
    )
    GROUP BY 1, 2, 3, 4

)
, total_supply as (
    SELECT day
        , symbol
        , atoken
        , SUM(supply_outstanding) OVER (PARTITION BY symbol, atoken, blockchain ORDER BY day) as supply
        , supply_outstanding as supply_change
    FROM (
        SELECT 
            series_data.day
            , series_data.atoken
            , series_data.symbol
            , series_data.blockchain
            , (COALESCE(s_m_a.minted, 0) - COALESCE(s_b_a.burnt, 0)) as supply_outstanding
        FROM series_data
        LEFT JOIN supply_minted_amount as s_m_a
            ON series_data.day = s_m_a.day and series_data.symbol = s_m_a.symbol
                AND series_data.atoken = s_m_a.atoken AND series_data.blockchain = s_m_a.blockchain
        LEFT JOIN supply_burn_amount as s_b_a
            ON series_data.day = s_b_a.day and series_data.symbol = s_b_a.symbol
                AND series_data.atoken = s_b_a.atoken AND series_data.blockchain = s_b_a.blockchain
    )
)
, debt_minted_amount as (

    SELECT day, symbol, vdtoken, blockchain, SUM(minted) as minted
    FROM (
        SELECT date_trunc('day', evt_block_time) as day
            , symbol, variableDebtToken as vdtoken
            , chain as blockchain
            , ((CAST(value AS INT256) - CAST(balanceIncrease AS INT256)) * POWER(10, -price_decimals))/(index * 1e-27) as minted
        FROM spark_protocol_multichain.variabledebttoken_evt_mint as evm_logs
            JOIN tokens on evm_logs.contract_address = tokens.variableDebtToken
        -- Mints
        WHERE evm_logs.evt_block_number >= tokens.creation_block
        and chain = 'ethereum'
    )
    GROUP BY 1, 2, 3, 4
)
, debt_burn_amount as (

    SELECT day, symbol, vdtoken, blockchain, SUM(burnt) as burnt
    FROM (
        SELECT date_trunc('day', evt_block_time) as day
            , symbol, variableDebtToken as vdtoken, chain as blockchain
            , ((CAST(value AS INT256) + CAST(balanceIncrease AS INT256)) * POWER(10, -price_decimals))/(index * 1e-27) as burnt
        FROM spark_protocol_multichain.variabledebttoken_evt_burn as evm_logs
            JOIN tokens on evm_logs.contract_address = tokens.variableDebtToken
        WHERE evm_logs.evt_block_number >= tokens.creation_block
        and chain = 'ethereum'
    )
    GROUP BY 1, 2, 3, 4
)
, total_debt as (
    SELECT day
        , symbol 
        , vdtoken
        , SUM(debt_outstanding) OVER (PARTITION BY symbol, vdtoken, blockchain ORDER BY day) as debt
        , debt_outstanding as debt_change
    FROM (
        SELECT 
            series_data.day
            , series_data.symbol
            , series_data.vdtoken
            , series_data.blockchain
            , (COALESCE(d_m_a.minted, 0) - COALESCE(d_b_a.burnt, 0)) as debt_outstanding
        FROM series_data
        LEFT JOIN debt_minted_amount as d_m_a
            ON series_data.day = d_m_a.day and series_data.symbol = d_m_a.symbol
                AND series_data.vdtoken = d_m_a.vdtoken AND series_data.blockchain = d_m_a.blockchain
        LEFT JOIN debt_burn_amount as d_b_a
            ON series_data.day = d_b_a.day and series_data.symbol = d_b_a.symbol
                AND series_data.vdtoken = d_b_a.vdtoken AND series_data.blockchain = d_b_a.blockchain
    )
)
, reserve_data as (
    
    
    WITH reserve_data as (
        SELECT date_trunc('day', evt_block_time) as time, reserve
            , POWER(1 + max_by(liquidityRate * 1e-27, evt_block_time)/31536000, 31536000) - 1 as supply_rate
            , max_by(liquidityIndex * 1e-27, evt_block_time) as liquidity_index, max_by(variableBorrowIndex * 1e-27, evt_block_time) as borrow_index
            , POWER(1 + max_by(variableBorrowRate * 1e-27, evt_block_time)/31536000, 31536000) - 1 as borrow_rate
        FROM spark_protocol_ethereum.Pool_evt_ReserveDataUpdated
        GROUP BY 1, 2
    
    )
    SELECT series_data.day
        , symbol, asset
        , COALESCE(supply_rate, LAST_VALUE(supply_rate) IGNORE NULLS OVER (PARTITION BY series_data.asset ORDER BY series_data.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) as supply_rate
        , COALESCE(liquidity_index, LAST_VALUE(liquidity_index) IGNORE NULLS OVER (PARTITION BY series_data.asset ORDER BY series_data.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) as liquidity_index
        , COALESCE(borrow_index, LAST_VALUE(borrow_index) IGNORE NULLS OVER (PARTITION BY series_data.asset ORDER BY series_data.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) as borrow_index
        , COALESCE(borrow_rate, LAST_VALUE(borrow_rate) IGNORE NULLS OVER (PARTITION BY series_data.asset ORDER BY series_data.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) as borrow_rate
    FROM
        series_data
        LEFT JOIN reserve_data on series_data.day = reserve_data.time and series_data.asset = reserve_data.reserve
)
, pricing as (

    SELECT dt as day, token_address as contract_address, t.symbol, price_usd
    FROM dune.steakhouse.result_token_price as p
        JOIN tokens as t on p.token_address = t.asset and p.blockchain = 'ethereum'

)
select tvl.symbol as symbol, tvl.day as period, aToken, vdToken
    , 'ethereum' as blockchain, 'spark' as protocol, '1' as version, 'main' as instance
    , ARRAY['wstETH', 'WETH', 'WBTC', 'USDC', 'rETH', 'GNO'] as collaterals -- only main ones
    , CASE WHEN supply_change = 0 then null else supply_change end as supply_delta_usd
    , CASE WHEN supply_change_tokens = 0 then null else supply_change_tokens end as delta_tokens
    , CASE WHEN debt_change = 0 then null else debt_change end as delta_borrow
    , case when deposit = 0 then null else price_usd * liquidity_index * deposit end as supply_usd
    , case when borrow = 0 then null else price_usd * borrow_index * borrow end as borrow_usd
    , case when deposit = 0 then null else liquidity_index * deposit end as supply_tokens
    , case when borrow = 0 then null else borrow_index * borrow end as borrow_tokens
    , case when deposit + borrow = 0 then null else price_usd * (deposit + borrow) end as total_usd
    , price_usd * borrow * CASE WHEN deposit <> 0 THEN borrow_index * borrow/(liquidity_index * deposit) END * borrow_rate / 365 as interest_paid_usd
    , liquidity_index as supply_index, borrow_index, supply_rate, borrow_rate
    , CASE 
        WHEN deposit <= 0 THEN NULL
        WHEN borrow = 0 then null
        ELSE borrow / deposit
    END  as utilization_ratio
FROM (
    SELECT series_data.symbol, series_data.day, series_data.aToken, series_data.vdToken, pricing.price_usd
        , SUM(total_supply.supply) as deposit
        , SUM(total_debt.debt) as borrow
        , SUM(total_debt.debt_change) * pricing.price_usd as debt_change
        , SUM(total_supply.supply_change) * pricing.price_usd as supply_change
        , SUM(total_supply.supply_change) as supply_change_tokens
    FROM series_data
        LEFT JOIN total_supply on series_data.day = total_supply.day and series_data.symbol = total_supply.symbol
        LEFT JOIN total_debt on series_data.day = total_debt.day and series_data.symbol = total_debt.symbol
        LEFT JOIN pricing on series_data.day = pricing.day and series_data.symbol = pricing.symbol
    GROUP BY 1, 2, 3, 4, pricing.price_usd
) as tvl
    LEFT JOIN reserve_data on tvl.day = reserve_data.day and tvl.symbol = reserve_data.symbol