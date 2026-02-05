/* 
MetaMorpho Utilisation Rates Base Per Market

References:
1) https://morpho.blockanalitica.com/metamorpho/
2) https://app.morpho.org/market?id={INSERT_ID_HERE}&network=mainnet

Changelog:
    2024-07-28: Initialise Query.
    2025-04-09: Revised Query with test query
    2025-04-13: Refactor query to remove unnecessary variables
    2025-04-23: Replaced markets test query by production query
    2025-04-28: Seb simplying and adding liquidity
    2025-05-04: Replaced the dt as creation_dt
    2025-07-15: Use Mat view of Morpho markets query
*/
with vaults as (
    -- Lists all vaults created
    select
        blockchain,
        vault_address
    from     dune.steakhouse.result_morpho_vaults
    -- query_4835775 -- Morpho - Vaults
)
, markets as (
    -- Lists all markets created
    select
        creation_dt,
        market_id,
        blockchain,
        coll_symbol,
        loan_decimals,
        coll_decimals
        FROM  dune.steakhouse.result_morpho_markets
        -- query_4847727 -- Morpho V2 - Markets
)
, vault_flows_daily as (
    SELECT date(evt_block_time) as dt
        , vault_address
        , blockchain
        , market_id
        , SUM(shares) as shares_delta
    FROM (
        SELECT evt_block_time, vault_address, chain as blockchain, supply.id as market_id, shares as shares
        FROM morpho_blue_multichain.morphoblue_evt_supply as supply JOIN vaults on supply.onBehalf = vaults.vault_address
        UNION ALL
        SELECT evt_block_time, vault_address, chain as blockchain, withdraw.id as market_id, -shares as shares
        FROM morpho_blue_multichain.morphoblue_evt_withdraw as withdraw JOIN vaults on withdraw.onBehalf = vaults.vault_address
    ) as vault_flows JOIN markets USING (market_id, blockchain)
    GROUP BY 1, 2, 3, 4
)
-- To Retrieve the Start Reallocation Time for MetaMorpho
, allocation_contracts as (
    SELECT vault_address, blockchain, market_id, MIN(dt) as start_dt
    FROM vault_flows_daily
    GROUP BY 1, 2, 3
)
, series_markets AS ( 
    SELECT dt
        , markets.coll_symbol
        , markets.blockchain
        , market_id
    FROM markets, unnest(sequence(date'2024-01-01', current_date, interval '1' day)) as s(dt)
    WHERE dt >= creation_dt
)
-- Creates a series for all markets, containing their relevant loan and collateral tokens associated with the market.
, series_markets_data as (
    SELECT dt
        , sm.coll_symbol
        , sm.blockchain
        , sm.market_id
        , ac.vault_address
    FROM series_markets as sm, allocation_contracts as ac
    WHERE dt >= ac.start_dt
        and ac.market_id = sm.market_id
        and ac.blockchain = sm.blockchain
)
, current_allocation_balance as (
    SELECT dt
        , blockchain
        , vault_address
        , market_id
        , coll_symbol
        , SUM(COALESCE(vfd.shares_delta, 0)) OVER (PARTITION BY vault_address, market_id ORDER BY dt) as shares_balance
        , SUM(COALESCE(vfd.shares_delta, 0)) OVER (PARTITION BY vault_address, market_id ORDER BY dt) * md.supply_index as token_balance
        , borrow_amount / supply_amount as utilization
        , supply_amount - borrow_amount as market_liquidity
    from series_markets_data as smd
    LEFT JOIN vault_flows_daily as vfd
            USING(dt, vault_address, market_id, blockchain)
    left join dune.steakhouse.result_morpho_markets_data md 
        using (dt, blockchain, market_id)
)
, current_allocations as (
    SELECT dt
        , blockchain
        , vault_address
        , market_id
        , markets.coll_symbol
        -- Converting shares from uint256 to decimal here
        , shares_balance * POWER(10, -loan_decimals - 6) * md.supply_index as token_balance
        , least(1, borrow_amount / supply_amount) as utilization
        , greatest(0, supply_amount - borrow_amount) as market_liquidity
        , supply_rate
    from series_markets_data as smd
    LEFT JOIN current_allocation_balance as cab
            USING(dt, vault_address, market_id, blockchain)
    left join dune.steakhouse.result_morpho_markets_data md 
        using (dt, blockchain, market_id)
    left join markets using (blockchain, market_id)
)
-- ***************************************************************************
-- ****************************    UTILISATIONS   ****************************
-- ***************************************************************************
SELECT dt
    , vault_address
    , blockchain
    , array_distinct(array_agg(coll_symbol)) as collaterals
    , SUM(token_balance) as token_balance
    , SUM(token_balance * utilization) / SUM(token_balance) as utilization_ratio -- Weighted based on Deposit Value
    , SUM(token_balance * supply_rate) / SUM(token_balance) as supply_rate -- Weighted based on Rates
    , sum(least(token_balance, market_liquidity)) as liquidity
FROM current_allocations
GROUP BY 1, 2, 3