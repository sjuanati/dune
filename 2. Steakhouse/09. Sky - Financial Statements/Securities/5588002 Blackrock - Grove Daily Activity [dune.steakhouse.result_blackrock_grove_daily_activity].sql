WITH tokens as (
    SELECT issuer, chain as blockchain, symbol, contract_address
        , label, asset_type, underlying, underlying_link
    from query_2453037
    WHERE symbol = 'BUIDL-I' and chain IN ('ethereum')
)
, date_series as (
    SELECT time, symbol, blockchain, contract_address, label, asset_type
        , underlying_link
    FROM unnest(sequence(
        TIMESTAMP'2025-04-06 00:00', 
        CAST(NOW() AS TIMESTAMP),
        interval '1' day)
    ) as s(time), tokens
)
, buidl_inflow_txs as (
    SELECT 
        block_time
        , bytearray_substring(input, bytearray_position(input, 0x00000000000000000160) - 32 - 10, 20)  as recipient
        , bytearray_to_uint256(bytearray_substring(input, bytearray_position(input, 0x000000000000000000000000000000000000000000000000000000000000002d) - 32 * 2, 32))  as value
        , tx_hash as evt_tx_hash
    FROM ethereum.traces
    WHERE success = true
    and tx_success = true
    and block_number >= 22218905 -- Apr-07-2025 06:47:23 PM
    and call_type = 'delegatecall'
    and bytearray_position(input, 0x39fadcec) = 1  -- bulkRegisterAndIssuance
)
, buidl_issued_interest as (
    SELECT block_time, tx_hash as evt_tx_hash
    FROM ethereum.traces
    WHERE success = true
    and tx_success = true
    and block_number >= 22218905 -- Apr-07-2025 06:47:23 PM
    and call_type = 'delegatecall'
    and bytearray_position(input, 0xb28d07c3) = 1  -- bulkIssuance
)
, transfer_txs as (
    SELECT DATE(t.evt_block_time) as time, t.contract_address
        , CASE 
            -- If there's an inflow 
            WHEN bit.value is not null THEN bit.value
            -- If there's a interest tx set as 0
            WHEN bii.evt_tx_hash is not null THEN 0
            -- If there's a outflow 
            WHEN t."from" = 0x491EDFB0B8b608044e227225C715981a30F3A44E THEN -t.value -- Grove Wallet
            ELSE t.value
        END/POWER(10,6) as amount
        , CASE WHEN bii.evt_tx_hash is not null THEN t.value ELSE 0 END/POWER(10,6) as interest
        , t.evt_tx_hash
    FROM blackrock_buidl_ethereum.dstoken_evt_transfer as t
        LEFT JOIN buidl_inflow_txs as bit ON t.evt_tx_hash = bit.evt_tx_hash and t.value = bit.value
        LEFT JOIN buidl_issued_interest as bii ON t.evt_tx_hash = bii.evt_tx_hash 
    WHERE 0x491EDFB0B8b608044e227225C715981a30F3A44E IN ("from", "to") -- Grove Wallet
)
, transfer_flows as (
    SELECT ds.time
        , ds.symbol, ds.contract_address
        , SUM(amount) as transfer
        , SUM(SUM(COALESCE(amount, 0))) OVER (PARTITION BY ds.contract_address, ds.symbol ORDER BY ds.time) as outstanding
        , SUM(interest) as interest
        , SUM(SUM(COALESCE(interest, 0))) OVER (PARTITION BY ds.contract_address, ds.symbol ORDER BY ds.time) as interest_total
    FROM date_series as ds 
        LEFT JOIN transfer_txs as t on t.time = ds.time
        and t.contract_address = ds.contract_address
    WHERE ds.time >= timestamp'2025-07-28 00:00'
    group by 1, 2, 3
)
/* COST BASIS */
, cost_basis as (
    -- Spark Transfers (Inflows and Outflows to Zerohash for redemptions/mints)
    -- Later transferred to Grove ownership (but cost basis remains the same)
    SELECT evt_block_time, (value/POWER(10, 6)) as pay_token_amount
    FROM circle_ethereum.usdc_evt_transfer
    -- Spark Liquidity Layer
    WHERE "from" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
        -- Blackrock Inflow USDC Address 
    and "to" = 0xD1917664bE3FdAea377f6E8D5BF043ab5C3b1312
    and evt_block_time <= timestamp'2025-07-19 00:00'
    UNION ALL
    SELECT  evt_block_time, -(value/POWER(10, 6)) as pay_token_amount
    FROM circle_ethereum.usdc_evt_transfer
        -- Spark Liquidity Layer
    WHERE "to" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
        --  Zerohash Address
    and "from" = 0xCfC0F98f30742B6d880f90155d4EbB885e55aB33
    and evt_block_time <= timestamp'2025-07-19 00:00'
    UNION ALL

    
    -- Grove Transfers (Inflows and Outflows to Zerohash for redemptions/mints)
    SELECT  evt_block_time, -(value/POWER(10, 6)) as pay_token_amount
    FROM circle_ethereum.usdc_evt_transfer
        --  Zerohash Address
    WHERE "from" = 0xCfC0F98f30742B6d880f90155d4EbB885e55aB33
        -- Grove 
    and "to" = 0x491EDFB0B8b608044e227225C715981a30F3A44E
    and evt_block_time >= timestamp'2025-07-18 21:11'
    UNION ALL
    SELECT  evt_block_time, (value/POWER(10, 6)) as pay_token_amount
    FROM circle_ethereum.usdc_evt_transfer
        -- Grove
    WHERE "to" = 0xCfC0F98f30742B6d880f90155d4EbB885e55aB33
        --  Zerohash Address 
    and "from" = 0x491EDFB0B8b608044e227225C715981a30F3A44E
    and evt_block_time >= timestamp'2025-07-18 21:11'
)
, cost_flows as (
    SELECT ds.time, SUM(SUM(COALESCE(pay_token_amount, 0))) OVER (ORDER BY ds.time) as allocated_usd
    FROM date_series as ds
        LEFT JOIN cost_basis as cb ON ds.time = date_trunc('day', cb.evt_block_time)
    GROUP BY 1 
)
SELECT time, symbol
    , MAX(contract_address) as contract_address
    , SUM(transfer) as transfer
    , SUM(outstanding) as outstanding
    , SUM(principal) as principal
    , SUM(interest_daily) as interest_daily
    , SUM(interest) as interest
    , 1 AS price, TRY_CAST(NULL AS DOUBLE) as index
    , max_by(allocated_usd, time) as allocated_usd
FROM (

    SELECT tf.time, tf.symbol
    , tf.contract_address
    , tf.transfer
    , tf.outstanding + interest_total as outstanding
    , CASE 
        WHEN (tf.outstanding + tf.interest_total) >= -1e-6 and (tf.outstanding + tf.interest_total) <=1e-6 THEN 0 -- If it is 0 (no balance)
        WHEN tf.outstanding < 0 THEN 0 -- Everything has transfered out
        ELSE tf.outstanding -- There is a balance
    END as principal
    , CASE WHEN interest > 0 then interest end as interest_daily
    , CASE 
        WHEN (tf.outstanding + tf.interest_total) >= -1e-6 and (tf.outstanding + tf.interest_total) <=1e-6 THEN 0 -- If it is 0 (no interest)
        WHEN tf.outstanding < 0 THEN (tf.outstanding + tf.interest_total) -- Total is the interest
        ELSE interest_total -- The is a balance 
    END as interest
    , allocated_usd
    FROM transfer_flows as tf
        JOIN cost_flows as cf on tf.time = cf.time
    WHERE outstanding is not null and tf.time >= TIMESTAMP'2025-04-01 00:00'
)
GROUP BY 1, 2