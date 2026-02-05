/*
@title: Morpho V2 - Markets Data
@description: Provides an overview of Morpho markets
@author: Steakhouse Financial
@references:
    1) https://morpho.blockanalitica.com/ethereum/markets
    2) https://legacy.morpho.org/market?id={MARKET_ID_HERE}&network=mainnet
    3) V1 query: https://dune.com/queries/3957276
@data validation:
    1) check shares for ethereum/base markets: etherscan/basescan -> 0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb -> market({ID})
@definitions:
    - Assets: amount of the underlying token (asset) that the contract holds (ie: USDC)
    - Shares: tokens issued by the market to represent a user’s proportional ownership of the underlying assets
    - Share Price: conversion rate between shares & assets that increases over time as interest accrues
                   gives how many underlying tokens (eg: USDT) correspond to 1 share
@translation to Lending Markets query:
    supply share price -> supply index
    borrow share price -> borrow index
@dev: amounts are not tracked because they don't include the accrued interest, so we use shares and share price instead to get to the current borrow & supply amounts
@version:
    - 1.0 - 2025-02-26 - Initial version
    - 2.0 - 2025-03-25 - Add new projection 
    - 3.0 - 2025-03-26 - Fix negative supply/borrow rate
                        -> Refactor code with definition on CTEs.
    - 4.0 - 2025-03-27 - Refactor code (separate CTEs for each section)
                        -> Add more comments per section
                        -> Tidy up CTEs
    - 5.0 - 2025-04-06 - Refactor names for columns and commas
    - 6.0 - 2025-04-15 - Remove CTEs relating to tracking, assigning, and collecting markets 
                        -> Add comments for share price precision rounding
    - 7.0 - 2025-04-22 - Add hardcoded values for protocol as 'morpho' and instance as 'main' and version 1
    - 8.0 - 2025-05-04 - Change dt to creation_dt
                        -> Remove usage of token_info dataset
    - 9.0 - 2025-06-06 - Add temp filter for chains
    - 10.0 - 2025-06-08 - Merge new query for share interpolations
    - 11.0 - 2025-07-15 - Use Mat view of Morpho markets query
    - 12.0 - 2025-11-05 - Use accounting_price_usd in coalesce for token_price
                        -> Update end stage of interpolation
    - 13.0 - 2026-02-02 - Update ending interpolation for borrow share price 
*/

-- ***************************************************************************
-- *******************               MARKETS               *******************
-- ***************************************************************************
with markets as (
    -- List of Morpho Markets assigned to it's collateral and loan tokens. 

        SELECT creation_dt
            , CAST(SUBSTRING(creation_ts, 1, 16) AS  TIMESTAMP) as creation_ts
            , market_id
            , blockchain
            , loan_address
            , loan_symbol
            , loan_decimals
            , coll_address
            , coll_symbol
            , coll_decimals
            , lltv
            , market_symbol as market_name
            , instance
        FROM  dune.steakhouse.result_morpho_markets
        -- query_4847727 -- Morpho V2 - Markets
    )
    -- Fetch Market Tokens Start Dates. 
    , market_tokens as (
        select blockchain
            , token_address
            , symbol
            , decimals
            , min(dt) as start_dt
        from (
            select blockchain, loan_address as token_address, loan_symbol as symbol, loan_decimals as decimals, min(creation_dt) as dt from markets group by 1, 2, 3, 4
            union all
            select blockchain, coll_address, coll_symbol, coll_decimals, min(creation_dt) as dt from markets group by 1, 2, 3, 4
        )
        group by 1, 2, 3, 4
    )

-- ***************************************************************************
-- *******************              PRICING                *******************
-- ***************************************************************************
    , pricing as (
        select p.dt
            , t.blockchain
            , t.token_address
            , t.decimals
            , t.symbol
            , coalesce(p.accounting_price_usd, p.price_usd) as price_usd
        from market_tokens t
        join dune.steakhouse.result_token_price p
            on t.blockchain = p.blockchain
            and t.token_address = p.token_address
        where p.dt >= t.start_dt
    ),

-- ***************************************************************************
-- *******************           SERIES DATA               *******************
-- ***************************************************************************
    -- Time series for markets since it's creation date.
    market_series as (
        select t.dt
            , m.blockchain
            , m.market_id
            , m.loan_address
            , m.loan_symbol
            , m.loan_decimals
            , m.coll_address
            , m.coll_symbol
            , m.coll_decimals
        from markets m
        cross join unnest(sequence(m.creation_dt, current_date, interval '1' day)) as t(dt)
    ),
-- ***************************************************************************
-- ****************************       SUPPLY      ****************************
-- ***************************************************************************
    supply_events as (
        -- SUPPLY events supplies asset tokens into market 
        select s.evt_block_time as ts
            , s.chain as blockchain
            , s.id as market_id
            , loan_decimals
            , s.shares / power(10, m.loan_decimals + 6) as shares
            , s.assets / power(10, m.loan_decimals) as amount
        from morpho_blue_multichain.morphoblue_evt_supply s
        join markets m
            on s.id = m.market_id
            and s.chain = m.blockchain
        union all

        -- WITHDRAW event exits supplied positions in market
        select w.evt_block_time as ts
            , w.chain as blockchain
            , w.id as market_id
            , loan_decimals
            , -(w.shares / power(10, m.loan_decimals + 6)) as shares
            , -(w.assets / power(10, m.loan_decimals)) as amount
        from morpho_blue_multichain.morphoblue_evt_withdraw w
        join markets m
            on w.id = m.market_id
            and w.chain = m.blockchain
    ),

-- ***************************************************************************
-- ****************************       BORROW      ****************************
-- ***************************************************************************
    borrow_events as (
        -- BORROW event increases borrower's liability 
        select b.evt_block_time as ts
            , b.chain as blockchain
            , b.id as market_id
            , loan_decimals
            , b.shares / power(10, m.loan_decimals + 6) as shares
            , (b.assets / power(10, m.loan_decimals)) as amount
            , evt_tx_hash
        from morpho_blue_multichain.morphoblue_evt_borrow b
        join markets m
            on b.id = m.market_id
            and b.chain = m.blockchain
        union all

        -- REPAY event reduces borrower's liability
        select r.evt_block_time as ts
            , r.chain as blockchain
            , r.id as market_id
            , loan_decimals
            , -(r.shares / power(10, m.loan_decimals + 6)) as shares
            , -(r.assets / power(10, m.loan_decimals)) as amount
            , evt_tx_hash
        from morpho_blue_multichain.morphoblue_evt_repay r
        join markets m
            on r.id = m.market_id
            and r.chain = m.blockchain
        union all

        -- LIQUIDATE event recovers assets and absorbs protocol bad debt
        select l.evt_block_time as ts
            , l.chain as blockchain
            , l.id as market_id
            , loan_decimals
            , -(l.repaidShares + badDebtShares) / power(10, m.loan_decimals + 6) as shares
            , -(l.repaidAssets + badDebtAssets) / power(10, m.loan_decimals) as amount
            , evt_tx_hash
        from morpho_blue_multichain.morphoblue_evt_liquidate l
        join markets m
            on l.id = m.market_id
            and l.chain = m.blockchain
    ),
-- ***************************************************************************
-- ****************************    COLLATERALS    ****************************
-- ***************************************************************************
    collateral_events as (
        -- SUPPLY COLLATERAL events supplies collateral tokens into market 
        select s.evt_block_time as ts
            , s.chain as blockchain
            , s.id as market_id
            , 0 as shares
            , s.assets / power(10, m.coll_decimals) as amount
        from morpho_blue_multichain.morphoblue_evt_supplycollateral s
        join markets m
            on s.id = m.market_id and s.chain = m.blockchain
        union all
        
        -- WITHDRAW COLLATERAL events withdraws collateral tokens into market 
        select w.evt_block_time as ts
            , w.chain as blockchain
            , w.id as market_id
            , 0 as shares
            , -(w.assets / power(10, m.coll_decimals)) as amount
        from morpho_blue_multichain.morphoblue_evt_withdrawcollateral w
        join markets m
            on w.id = m.market_id and w.chain = m.blockchain
        union all

        -- LIQUIDATE events sizes assets from positions into market 
        select l.evt_block_time as ts
            , l.chain as blockchain
            , l.id as market_id
            , 0 as shares
            , -(l.seizedAssets / power(10, m.coll_decimals)) as amount
        from morpho_blue_multichain.morphoblue_evt_liquidate l
        join markets m
            on l.id = m.market_id and l.chain = m.blockchain
    )
-- ***************************************************************************
-- ************************     ACCRUE  INTEREST     *************************
-- ***************************************************************************
    , accrue_interest_events as (
        SELECT evt_block_time as ts
            , blockchain
            , ai.id AS market_id
            , 0 as shares
            , interest * power(10, -m.loan_decimals) as interest
        FROM morpho_blue_multichain.MorphoBlue_evt_AccrueInterest as ai
            JOIN markets as m on ai.id = m.market_id 
    )
-- ***************************************************************************
-- ************************    BORROW RATE EVENTS    *************************
-- ***************************************************************************
    , borrow_rates_events as (
        select evt_block_time as ts
            , chain as blockchain
            , id as market_id
            , (avgBorrowRate / power(10, 18)) as borrow_rate
        from morpho_blue_multichain.adaptivecurveirm_evt_borrowrateupdate
    )
    , daily_rates_filled as (
        SELECT ms.dt, ms.blockchain
            , ms.market_id, borrow_rate
            , row_number() over (partition by ms.dt, ms.market_id, ms.blockchain order by bre.ts desc) as rn
        FROM market_series as ms
            left join borrow_rates_events as bre on ms.market_id = bre.market_id 
                and ms.blockchain = bre.blockchain and ms.dt >= bre.ts
    )
    , daily_rates_filled_periods as (
        select dt, blockchain, market_id, borrow_rate
            -- Convert rate (in Secs form) in Daily form (60 * 60 * 24) for all previous dates
            , COALESCE(SUM(borrow_rate * 86400) OVER (PARTITION BY blockchain, market_id ORDER BY dt ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) as cumulative_interest_midnight
        from daily_rates_filled where rn = 1
    )
-- ***************************************************************************
-- *************************     SHARE  PRICING     **************************
-- ***************************************************************************
    -- calculate price of supplied and borrowed loan token amounts based on assets/shares formula
    , shares_pricing as (
        SELECT ts
            , blockchain
            , market_id
            , side
            -- Rounding to solve floating precision errors.
            , round(abs(amount) / abs(shares), 15) as share_price
            , to_unixtime(ts) as share_unix_ts
        from (
            select ts, blockchain, market_id, shares, amount, 'supply' as side from supply_events
            where abs(amount) * power(10, loan_decimals) >= 1.5e6
            union all
            select ts, blockchain, market_id, shares, amount, 'borrow' as side from borrow_events
            where abs(amount) * power(10, loan_decimals) >= 1.5e6
        )       
        -- WHERE amount >= 1.5e-2  -- Filter for diff collateral token if it's WETH or WBTC
        union all
        -- At creation markets all supplies/borrows start at share price 1.0.  
        select creation_ts as ts
            , blockchain
            , market_id
            , side
            , 1.0 as share_price
            , to_unixtime(creation_ts) as share_unix_ts
        from markets
        cross join (values ('supply'), ('borrow')) as t(side)
        WHERE (t.side = 'borrow' and coll_address != 0x0000000000000000000000000000000000000000)  -- Idle Markets
        or t.side = 'supply'
    ),
    -- transpose query to get borrow price, borrow timestamp, supply price & supply timestamp per market
    borrow_shares_pricing_pivot as (
        -- unique results for shares events
        select sp.ts
            , 'event' as row_type
            , sp.blockchain
            , sp.market_id
            , keccak(to_utf8(CAST(sp.market_id as VARCHAR) || sp.blockchain)) as market_hash
            , MAX(share_unix_ts) as borrow_unix_ts
            , MAX(share_price) as borrow_share_price
            -- Interest up until Midnight + (daily rate * seconds since Midnight)
            , MAX(drfp.cumulative_interest_midnight + (drfp.borrow_rate * (share_unix_ts - to_unixtime(drfp.dt)))) as event_cumulative_value
        from shares_pricing as sp
            LEFT JOIN daily_rates_filled_periods as drfp on date_trunc('day', sp.ts) = drfp.dt
            and sp.blockchain = drfp.blockchain and sp.market_id = drfp.market_id 
        WHERE side = 'borrow'
        GROUP BY 1, 3, 4
    ),
    -- transpose query to get supply price, supply timestamp, supply price & supply timestamp per market
    supply_shares_pricing_pivot as (
        -- unique results for shares events
        select ts
            , 'event' as row_type
            , blockchain
            , market_id
            , keccak(to_utf8(CAST(market_id as VARCHAR) || blockchain)) as market_hash
            , MAX(share_unix_ts) as supply_unix_ts
            , MAX(share_price) as supply_share_price
        from shares_pricing
        WHERE side = 'supply'
        GROUP BY 1, 3, 4
    ),
-- ***************************************************************************
-- *************************     EVENT PERIODS     ***************************
-- ***************************************************************************
    borrow_event_periods as (
        select
            market_id,
            blockchain,
            -- Previous Previous borrow ts and share price.
            LAG(borrow_unix_ts, 1) IGNORE NULLS OVER (PARTITION BY market_hash ORDER BY ts) as previous_previous_borrow_ts,
            LAG(borrow_share_price, 1) IGNORE NULLS OVER (PARTITION BY market_hash ORDER BY ts) as previous_previous_borrow_price,

            -- Treat current ts and price as previous.
            borrow_unix_ts as previous_borrow_ts,
            borrow_share_price as previous_borrow_price,
            
            LAG(event_cumulative_value, 1) IGNORE NULLS OVER (PARTITION BY market_hash ORDER BY ts) as previous_event_cumulative_value,

            -- Next borrow ts and share price.
            LEAD(borrow_unix_ts, 1) IGNORE NULLS OVER (PARTITION BY market_hash ORDER BY ts) as next_borrow_ts,
            LEAD(borrow_share_price, 1) IGNORE NULLS OVER (PARTITION BY market_hash ORDER BY ts) as next_borrow_price,

            -- Next Next Borrow ts and share price.
            LEAD(borrow_unix_ts, 2) IGNORE NULLS OVER (PARTITION BY market_hash ORDER BY ts) as next_next_borrow_ts,
            LEAD(borrow_share_price, 2) IGNORE NULLS OVER (PARTITION BY market_hash ORDER BY ts) as next_next_borrow_price
    
        FROM borrow_shares_pricing_pivot
    ),
    supply_event_periods as (
        select
            market_id,
            blockchain,
            -- Previous Previous supply ts and share price.
            LAG(supply_unix_ts, 1) IGNORE NULLS OVER (PARTITION BY market_hash ORDER BY ts) as previous_previous_supply_ts,
            LAG(supply_share_price, 1) IGNORE NULLS OVER (PARTITION BY market_hash ORDER BY ts) as previous_previous_supply_price,

            -- Treat current ts and price as previous.
            supply_unix_ts as previous_supply_ts,
            supply_share_price as previous_supply_price,

            -- Next supply ts and share price.
            LEAD(supply_unix_ts, 1) IGNORE NULLS OVER (PARTITION BY market_hash ORDER BY ts) as next_supply_ts,
            LEAD(supply_share_price, 1) IGNORE NULLS OVER (PARTITION BY market_hash ORDER BY ts) as next_supply_price,

            -- Next Next supply ts and share price.
            LEAD(supply_unix_ts, 2) IGNORE NULLS OVER (PARTITION BY market_hash ORDER BY ts) as next_next_supply_ts,
            LEAD(supply_share_price, 2) IGNORE NULLS OVER (PARTITION BY market_hash ORDER BY ts) as next_next_supply_price
    
        FROM supply_shares_pricing_pivot
    ),
    daily_filled_supply_periods as (
        select
            s.dt,
            s.market_id,
            s.blockchain,
            
            previous_previous_supply_ts, previous_previous_supply_price,
            previous_supply_ts, previous_supply_price,
            next_supply_ts, next_supply_price,
            next_next_supply_ts, next_next_supply_price,
            -- Ranks the latest price for each day.
            -- 1. Being the latest
            row_number() over (partition by s.dt, s.market_id, s.blockchain order by p.previous_supply_ts desc) as rn
        from market_series s
        left join supply_event_periods p
            on s.market_id = p.market_id
            and s.blockchain = p.blockchain
            and to_unixtime(s.dt) >= p.previous_supply_ts
    )
    , daily_supply_periods as (
        select * from daily_filled_supply_periods where rn = 1
    ), 
    daily_filled_borrow_periods as (
        select
            s.dt,
            s.market_id,
            s.blockchain,
            
            previous_previous_borrow_ts, previous_previous_borrow_price,
            previous_borrow_ts, previous_borrow_price, previous_event_cumulative_value,
            next_borrow_ts, next_borrow_price,
            next_next_borrow_ts, next_next_borrow_price,
            -- Ranks the latest price for each day.
            -- 1. Being the latest
            row_number() over (partition by s.dt, s.market_id, s.blockchain order by p.previous_borrow_ts desc) as rn
        from market_series s
        left join borrow_event_periods p
            on s.market_id = p.market_id
            and s.blockchain = p.blockchain
            and to_unixtime(s.dt) >= p.previous_borrow_ts
    )
    , daily_borrow_periods as (
        select * from daily_filled_borrow_periods where rn = 1
    )

-- ***************************************************************************
-- **********************     SHARE  INTERPOLATION     ***********************
-- ***************************************************************************
    , share_price_series as (
        select
            dsp.dt,
            dsp.market_id,
            dsp.blockchain,
            CASE
                -- The Base Case: Interpolate two points from the next and previous position.
                WHEN previous_supply_price IS NOT NULL AND next_supply_price IS NOT NULL
                    THEN previous_supply_price + (next_supply_price - previous_supply_price)/(next_supply_ts - previous_supply_ts) * (to_unixtime(dsp.dt) - previous_supply_ts)

                -- Derive interpolation from the future prices (using the change from next next price and next price).
                -- This is for events where we have nulls in the beginning.
                WHEN next_supply_price IS NOT NULL AND next_next_supply_price IS NOT NULL
                    THEN GREATEST(1, next_supply_price - (next_next_supply_price - next_supply_price)/(next_next_supply_ts - next_supply_ts) * (next_supply_ts - to_unixtime(dsp.dt)))

                -- Derive interpolation from previous prices (using the change from the previous previous price and previous price).
                --This is for events where nulls are at the end.
                WHEN previous_supply_price IS NOT NULL AND previous_previous_supply_price IS NOT NULL
                    THEN previous_supply_price + (previous_supply_price - previous_previous_supply_price)/(previous_supply_ts - previous_previous_supply_ts) * (to_unixtime(dsp.dt) - previous_supply_ts)

                -- Catch Case 
                --> Either extract current price or the next price.
                ELSE COALESCE(previous_supply_price, next_supply_price)
            END as supply_share_price
            , CASE
                -- The Base Case: Interpolate two points from the next and previous position.
                WHEN previous_borrow_price IS NOT NULL AND next_borrow_price IS NOT NULL
                    THEN previous_borrow_price + (next_borrow_price - previous_borrow_price)/(next_borrow_ts - previous_borrow_ts) * (to_unixtime(dsp.dt) - previous_borrow_ts)

                -- Derive interpolation from the future prices (using the change from next next price and next price).
                -- This is for events where we have nulls in the beginning.
                WHEN next_borrow_price IS NOT NULL AND next_next_borrow_price IS NOT NULL
                    THEN GREATEST(1, next_borrow_price - (next_next_borrow_price - next_borrow_price)/(next_next_borrow_ts - next_borrow_ts) * (next_borrow_ts - to_unixtime(dsp.dt)))

                -- If we have a previous price and valid accumulators to calculator growth  tracking the compounding effect of the share price
                -- Formula: Price * exp(Current Total Interest - Previous Event Total Interest) 
                WHEN previous_borrow_price IS NOT NULL AND cumulative_interest_midnight IS NOT NULL
                    -- THEN previous_borrow_price * exp((borrow_rate) * (to_unixtime(dsp.dt) - previous_borrow_ts))
                    THEN previous_borrow_price * exp(cumulative_interest_midnight - dsb.previous_event_cumulative_value)
                -- -- Derive interpolation from previous prices (using the change from the previous previous price and previous price).
                -- --This is for events where nulls are at the end.
                -- WHEN previous_borrow_price IS NOT NULL AND previous_previous_borrow_price IS NOT NULL AND previous_borrow_ts != previous_previous_borrow_ts
                --     THEN previous_borrow_price + (previous_borrow_price - previous_previous_borrow_price)/(previous_borrow_ts - previous_previous_borrow_ts) * (to_unixtime(dsp.dt) - previous_borrow_ts)

                -- Catch Case 
                --> Either extract current price or the next price.
                ELSE COALESCE(previous_borrow_price, next_borrow_price)
            END as borrow_share_price
        from daily_supply_periods as dsp
        LEFT JOIN daily_borrow_periods as dsb
            ON dsp.dt = dsb.dt and dsp.blockchain = dsb.blockchain and dsp.market_id = dsb.market_id
        LEFT JOIN daily_rates_filled_periods as drfp
            ON dsp.dt = drfp.dt and dsp.blockchain = drfp.blockchain and dsp.market_id = drfp.market_id
    )
    , final_share_price_daily as (
        -- Interpolated daily
        SELECT dt -- Always midnight date
            , market_id, blockchain
            , supply_share_price
            , borrow_share_price
            , LAG(supply_share_price) IGNORE NULLS OVER (partition by blockchain, market_id ORDER BY dt) as prev_supply_share_price
            , LAG(borrow_share_price) IGNORE NULLS OVER (partition by blockchain, market_id ORDER BY dt) as prev_borrow_share_price
        FROM share_price_series
        WHERE supply_share_price is not null
        union all
        select m.creation_dt
            , m.market_id
            , m.blockchain
            , 1, 1, NULL, NULL
        from markets m
    )
    , market_flows_daily as (
        select date_trunc('day', ts) as dt,
            blockchain,
            market_id,
            SUM(if(side = 'borrow', shares, 0)) as borrow_delta_shares,
            SUM(if(side = 'supply', shares, 0)) as supply_delta_shares,
            SUM(if(side = 'collateral', amount, 0)) as coll_delta_amount,
            SUM(if(side = 'interest', amount, 0)) as interest_amount
        from (
            select ts, blockchain, market_id, shares, amount, 'supply' as side from supply_events
            union all
            select ts, blockchain, market_id, shares, amount, 'borrow' as side from borrow_events
            union all
            select ts, blockchain, market_id, shares, amount, 'collateral' as side from collateral_events
            union all
            select ts, blockchain, market_id, shares, interest, 'interest' as side from accrue_interest_events
        )
        GROUP BY 1, 2, 3
    )
    , market_balance_daily as (
        select s.dt
            , s.blockchain
            , s.market_id
            , s.loan_address
            , s.coll_address
            , COALESCE(d.supply_delta_shares, 0) as supply_delta_shares
            , COALESCE(d.borrow_delta_shares, 0) as borrow_delta_shares
            , COALESCE(interest_amount, 0) as interest_amount
            , sum(coalesce(d.supply_delta_shares, 0)) over (partition by s.blockchain, s.market_id order by s.dt) as supply_total_shares
            , sum(coalesce(d.borrow_delta_shares, 0)) over (partition by s.blockchain, s.market_id order by s.dt) as borrow_total_shares
            , sum(coalesce(d.coll_delta_amount, 0)) over (partition by s.blockchain, s.market_id order by s.dt) as coll_total_amount
        from market_series s
        left join market_flows_daily d
            on s.dt = d.dt and s.market_id = d.market_id and s.blockchain = d.blockchain 
    )
    , market_daily as (
        select
            m.dt
            , m.blockchain
            , m.market_id
            , p_l.price_usd as loan_price
            , p_c.price_usd as coll_price
            , s.supply_share_price
            , s.borrow_share_price

            , m.supply_total_shares
            , m.borrow_total_shares
            , s.prev_supply_share_price
            , s.prev_borrow_share_price
            
            , s.supply_share_price * m.supply_delta_shares as supply_delta_amount
            , s.borrow_share_price * m.borrow_delta_shares as borrow_delta_amount
            , s.supply_share_price * m.supply_total_shares as supply_total_amount
            , s.borrow_share_price * m.borrow_total_shares as borrow_total_amount
            , m.coll_total_amount
            , m.interest_amount
        from market_balance_daily m
        join final_share_price_daily s 
            on m.dt = s.dt and m.market_id = s.market_id and m.blockchain = s.blockchain
        left join pricing p_l -- pricing for loan asset
            on m.dt = p_l.dt and m.loan_address = p_l.token_address and m.blockchain = p_l.blockchain
        left join pricing p_c -- pricing for collateral asset
            on m.dt = p_c.dt and m.coll_address = p_c.token_address and m.blockchain = p_c.blockchain
    )
    , morpho_markets as (
        select dt
            , blockchain
            , market_id

            -- Prices
            , loan_price
            , coll_price

            -- Loan and Collateral Tokens Amounts
            , supply_total_amount
            , borrow_total_amount
            , supply_delta_amount
            , interest_amount as interest_paid_amount
            , coll_total_amount

            -- Shares
            , supply_total_shares
            , borrow_total_shares

            -- USD Values
            , loan_price * supply_delta_amount as supply_delta_usd
            , loan_price * borrow_delta_amount as borrow_delta_usd
            , coll_price * coll_total_amount as coll_total_usd
            , loan_price * supply_total_amount as supply_usd
            , loan_price * borrow_total_amount as borrow_usd
            , loan_price * supply_total_amount - loan_price * borrow_total_amount + coll_price * coll_total_amount as total_usd
            , loan_price * interest_amount as interest_paid_usd
            -- Rates
            , ((supply_share_price / prev_supply_share_price) - 1) * 365 as supply_rate
            , ((borrow_share_price / prev_borrow_share_price) - 1) * 365 as borrow_rate
            , case
                when (loan_price * supply_total_amount) = 0 then null
                when supply_total_amount < 1e-6 or borrow_total_amount < 1-6 then 0
                when (loan_price * borrow_total_amount) > (loan_price * supply_total_amount) then 1 -- markets under preassure (ie: borrowers > suppliers)
                else (loan_price * borrow_total_amount) / (loan_price * supply_total_amount) -- normal
            end as utilization_ratio

            -- Share Price equivalent to Indices
            , supply_share_price as supply_index
            , borrow_share_price as borrow_index
        from market_daily
    )
    , morpho_markets_final as (
        select dt
            , blockchain
            , 'morpho' as protocol
            , instance
            , '1' as version
            , market_id
            , market_name
            , loan_symbol as symbol
            , coll_symbol as collateral
            , loan_price
            , coll_price

            , CASE WHEN supply_total_shares > 1e-10 THEN supply_total_shares ELSE 0 END as supply_shares
            , CASE WHEN borrow_total_shares > 1e-10 THEN borrow_total_shares ELSE 0 END as borrow_shares
            , interest_paid_amount

            , CASE WHEN abs(supply_delta_usd) > 1e-6 THEN supply_delta_usd ELSE 0 END as supply_delta_usd
            , CASE WHEN abs(borrow_delta_usd) > 1e-6 then borrow_delta_usd ELSE 0 END  as borrow_delta_usd
            , CASE WHEN coll_total_usd > 1e-6 THEN coll_total_usd ELSE 0 END as coll_usd
            , CASE WHEN supply_usd > 1e-6 THEN supply_usd ELSE 0 end as supply_usd
            , CASE WHEN borrow_usd > 1e-6 THEN borrow_usd ELSE 0 END as borrow_usd
            , CASE WHEN total_usd > 1e-6 THEN total_usd ELSE 0 END as tvl_usd
            , interest_paid_usd

            , CASE WHEN supply_total_amount > 1e-10 THEN supply_total_amount ELSE 0 END as supply_amount
            , CASE WHEN borrow_total_amount > 1e-10 then borrow_total_amount else 0 END as borrow_amount
            , CASE WHEN coll_total_amount > 1e-10 THEN coll_total_amount ELSE 0 END as coll_amount
            , CASE WHEN abs(supply_delta_amount) > 1e-6 THEN supply_delta_amount ELSE 0 END as supply_delta_amount
            , supply_rate
            , borrow_rate
            , utilization_ratio
            , supply_index
            , borrow_index
        from morpho_markets
            JOIN markets USING (market_id, blockchain)
    )
select * from morpho_markets_final order by dt desc
