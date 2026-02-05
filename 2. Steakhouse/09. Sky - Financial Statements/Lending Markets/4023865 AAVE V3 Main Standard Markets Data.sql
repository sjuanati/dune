-- Aave V3 Markets identifies all relevant markets, all chains, and the metrics associated with it.
-- References:
-- 1) https://defillama.com/protocol/aave-v3
-- 2) https://app.aave.com/markets/
-- Changelog:
-- 2024-08-26: Create query.
--              -> Resolve gapped values with using last_value function in reserve updates.
-- 2024-11-23: Track supply and borrows in terms of the atoken values (which index the token amounts)
-- 2024-12-12: Add ZKSync integration
--              -> Change Pool Configuration to evms.log  
--              -> Fix series to be partitioned by asset and blockchain.
-- 2024-12-13: Refactor code.
-- 2024-12-15: Add collaterals per chain not hardcoded.
--              -> Remove old configurator setup. 
--              -> Change symbols to actual symbols
-- 2024-12-17: Add sDAI pricing. 
-- 2025-03-21: Add balanceIncrease into supply/burns for mints/burns to correct the final calc. 
-- 2025-04-22: Change column names to have correct suffixes (_usd, _tokens)
-- 2025-08-27: Add vdToken and aToken as final result
-- 2025-09-03: Add horizon platform
with days AS ( 
    SELECT day
    FROM unnest(sequence(
        CASE '{{chain}}'
            WHEN 'ethereum' THEN timestamp'2023-01-27 00:00:00'
            WHEN 'arbitrum' THEN timestamp'2022-03-11 00:00:00'
            WHEN 'scroll' THEN timestamp'2024-02-09 00:00:00'
            WHEN 'polygon' THEN timestamp'2022-03-11 00:00:00'
            WHEN 'avalanche_c' THEN timestamp'2022-03-11 00:00:00'
            WHEN 'base' THEN timestamp'2023-08-22 00:00:00'
            WHEN 'avalanche_c' THEN timestamp'2022-03-11 00:00:00'
            WHEN 'optimism' THEN timestamp'2022-03-11 00:00:00'
            WHEN 'bnb' THEN timestamp'2024-01-23 00:00:00'
            WHEN 'gnosis' THEN timestamp'2023-11-07 00:00:00'
            WHEN 'zksync' THEN timestamp'2024-09-20 00:00:00'
        END, 
        CAST(NOW() as timestamp),
        interval '1' day)
    ) as s(day)
)
, tokens as (

    WITH pool_configurators(blockchain, instance, contract_address, block_number) as (
        VALUES
        ('ethereum', 'main', 0x64b761D848206f447Fe2dd461b0c635Ec39EbB27, 16291130),
        ('bnb', 'main', 0x67bdF23C7fCE7C65fF7415Ba3F2520B45D6f9584, 33571625),
        ('polygon', 'main', 0x8145eddDf43f50276641b55bd3AD95944510021E, 25826031),
        ('avalanche_c', 'main', 0x8145eddDf43f50276641b55bd3AD95944510021E, 11970516),
        ('gnosis', 'main', 0x7304979ec9E4EaA0273b6A037a31c4e9e5A75D16, 30293057),
        ('optimism', 'main', 0x8145eddDf43f50276641b55bd3AD95944510021E, 4365702),
        ('arbitrum', 'main', 0x8145eddDf43f50276641b55bd3AD95944510021E, 7742433),
        ('scroll', 'main', 0x32BCab42a2bb5AC577D24b425D46d8b8e0Df9b7f, 2618764),
        ('base', 'main', 0x5731a04B1E775f0fdd454Bf70f3335886e9A96be, 2357134),
        ('zksync', 'main', 0x0207d31b4377C74bEC37356aaD83E3dCc979F40E, 43709029),
        ('ethereum', 'ether.fi', 0x8438F4D29D895d75C86BDC25360c25eF0607E65d, 20625519),
        ('ethereum', 'prime', 0x342631c6CeFC9cfbf97b2fe4aa242a236e1fd517, 20262414),
        ('ethereum', 'horizon', 0x83Cb1B4af26EEf6463aC20AFbAC9c0e2E017202F, 23125535)
    )

    SELECT all_tokens.symbol
        , '{{chain}}' as blockchain
        , decimals as price_decimals
        , bytearray_substring(topic1, 1 + 12, 20) as asset
        , bytearray_substring(topic2, 1 + 12, 20) as atoken
        , bytearray_substring("data", 1 + 12, 20) as stableDebtToken
        , bytearray_substring("data", 1 + 12 + 32, 20) as variableDebtToken
        , evm_logs.block_number as creation_block
        , evm_logs.block_time as creation_time
        , evm_logs.tx_hash
    FROM pool_configurators JOIN evms.logs AS evm_logs 
            on pool_configurators.contract_address = evm_logs.contract_address 
            and evm_logs.blockchain = pool_configurators.blockchain
        JOIN dune.steakhouse.result_token_info as all_tokens
            on all_tokens.token_address = bytearray_substring(evm_logs.topic1, 1 + 12, 20)
            and all_tokens.blockchain = '{{chain}}'
    WHERE topic0 = 0x3a0ca721fc364424566385a1aa271ed508cc2c0949c2272575fb3013a163a45f
        and evm_logs.block_number >= pool_configurators.block_number
        and evm_logs.blockchain = '{{chain}}'
        and pool_configurators.instance = '{{instance}}'
)
, series_data as (
    SELECT days.day, tokens.asset, tokens.aToken, tokens.variableDebtToken as vdToken, tokens.blockchain, tokens.symbol
    FROM tokens, days
    WHERE date(days.day) >= date(creation_time)
    GROUP BY 1, 2, 3, 4, 5, 6
)
, supply_minted_amount as (

    SELECT day, symbol, asset, blockchain, SUM(minted) as minted
    FROM (
        SELECT date_trunc('day', block_time) as day
            , symbol, asset
            , evm_logs.blockchain
            , ((bytearray_to_int256(bytearray_substring(data, 1, 32)) - bytearray_to_int256(bytearray_substring(data, 1 + 32, 32))) * POWER(10, -price_decimals))/(bytearray_to_int256(bytearray_substring(data, 1 + 32 * 2, 32)) * 1e-27) as minted
        FROM evms.logs as evm_logs
            JOIN tokens on evm_logs.contract_address = tokens.atoken
            and evm_logs.blockchain = tokens.blockchain
        -- Mints
        WHERE topic0 = 0x458f5fa412d0f69b08dd84872b0215675cc67bc1d5b6fd93300a1c3878b86196
            and evm_logs.block_number >= tokens.creation_block
    )
    GROUP BY 1, 2, 3, 4

)
, supply_burn_amount as (

    SELECT day, symbol, asset, blockchain, SUM(burnt) as burnt
    FROM (
        SELECT date_trunc('day', block_time) as day
            , asset
            , symbol
            , evm_logs.blockchain
            , ((bytearray_to_int256(bytearray_substring(data, 1, 32)) + bytearray_to_int256(bytearray_substring(data, 1 + 32, 32))) * POWER(10, -price_decimals))/(bytearray_to_int256(bytearray_substring(data, 1 + 32 * 2, 32)) * 1e-27) as burnt
        FROM evms.logs as evm_logs
            JOIN tokens on evm_logs.contract_address = tokens.atoken
            and evm_logs.blockchain = tokens.blockchain
        -- Mints
        WHERE topic0 = 0x4cf25bc1d991c17529c25213d3cc0cda295eeaad5f13f361969b12ea48015f90
            and evm_logs.block_number >= tokens.creation_block
    )
    GROUP BY 1, 2, 3, 4

)
, total_supply as (
    SELECT day
        , symbol
        , asset
        , SUM(supply_outstanding) OVER (PARTITION BY symbol, asset, blockchain ORDER BY day) as supply
        , supply_outstanding as supply_change
    FROM (
        SELECT 
            series_data.day
            , series_data.asset
            , series_data.symbol
            , series_data.blockchain
            , (COALESCE(s_m_a.minted, 0) - COALESCE(s_b_a.burnt, 0)) as supply_outstanding
        FROM series_data
        LEFT JOIN supply_minted_amount as s_m_a
            ON series_data.day = s_m_a.day and series_data.symbol = s_m_a.symbol
                AND series_data.asset = s_m_a.asset AND series_data.blockchain = s_m_a.blockchain
        LEFT JOIN supply_burn_amount as s_b_a
            ON series_data.day = s_b_a.day and series_data.symbol = s_b_a.symbol
                AND series_data.asset = s_b_a.asset AND series_data.blockchain = s_b_a.blockchain
    )
)
, debt_minted_amount as (

    SELECT day, symbol, asset, blockchain, SUM(minted) as minted
    FROM (
        SELECT date_trunc('day', block_time) as day
            , symbol
            , asset
            , evm_logs.blockchain
            , ((bytearray_to_int256(bytearray_substring(data, 1, 32)) - bytearray_to_int256(bytearray_substring(data, 1 + 32, 32))) * POWER(10, -price_decimals))/(bytearray_to_int256(bytearray_substring(data, 1 + 32 * 2, 32)) * 1e-27) as minted
        FROM evms.logs as evm_logs
            JOIN tokens on evm_logs.contract_address = tokens.variableDebtToken
            and evm_logs.blockchain = tokens.blockchain
        -- Mints
        WHERE topic0 = 0x458f5fa412d0f69b08dd84872b0215675cc67bc1d5b6fd93300a1c3878b86196
            and evm_logs.block_number >= tokens.creation_block
    )
    GROUP BY 1, 2, 3, 4
)
, debt_burn_amount as (

    SELECT day, symbol, asset, blockchain, SUM(burnt) as burnt
    FROM (
        SELECT date_trunc('day', block_time) as day
            , symbol, asset, evm_logs.blockchain
            , ((bytearray_to_int256(bytearray_substring(data, 1, 32)) + bytearray_to_int256(bytearray_substring(data, 1 + 32, 32))) * POWER(10, -price_decimals))/(bytearray_to_int256(bytearray_substring(data, 1 + 32 * 2, 32)) * 1e-27) as burnt
        FROM evms.logs as evm_logs
            JOIN tokens on evm_logs.contract_address = tokens.variableDebtToken
            and evm_logs.blockchain = tokens.blockchain
        WHERE topic0 = 0x4cf25bc1d991c17529c25213d3cc0cda295eeaad5f13f361969b12ea48015f90
            and evm_logs.block_number >= tokens.creation_block
    )
    GROUP BY 1, 2, 3, 4
)
, total_debt as (
    SELECT day
        , symbol 
        , asset
        , SUM(debt_outstanding) OVER (PARTITION BY symbol, asset, blockchain ORDER BY day) as debt
        , debt_outstanding as debt_change
    FROM (
        SELECT 
            series_data.day
            , series_data.symbol
            , series_data.asset
            , series_data.blockchain
            , (COALESCE(d_m_a.minted, 0) - COALESCE(d_b_a.burnt, 0)) as debt_outstanding
        FROM series_data
        LEFT JOIN debt_minted_amount as d_m_a
            ON series_data.day = d_m_a.day and series_data.symbol = d_m_a.symbol
                AND series_data.asset = d_m_a.asset AND series_data.blockchain = d_m_a.blockchain
        LEFT JOIN debt_burn_amount as d_b_a
            ON series_data.day = d_b_a.day and series_data.symbol = d_b_a.symbol
                AND series_data.asset = d_b_a.asset AND series_data.blockchain = d_b_a.blockchain
    )
)
, reserve_markets(blockchain, instance, contract_address, block_number) as (
    VALUES
    ('ethereum', 'main', 0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2, 16496817),
    ('bnb', 'main', 0x6807dc923806fe8fd134338eabca509979a7e0cb, 35498575),
    ('polygon', 'main', 0x794a61358D6845594F94dc1DB02A252b5b4814aD, 25873345),
    ('avalanche_c', 'main', 0x794a61358d6845594f94dc1db02a252b5b4814ad, 12027673),
    ('gnosis', 'main', 0xb50201558b00496a145fe76f7424749556e326d8, 30834029),
    ('optimism', 'main', 0x794a61358d6845594f94dc1db02a252b5b4814ad, 4477794),
    ('arbitrum', 'main', 0x794a61358d6845594f94dc1db02a252b5b4814ad, 7998442),
    ('scroll', 'main', 0x11fcfe756c05ad438e312a7fd934381537d3cffe, 3196633),
    ('base', 'main', 0xa238dd80c259a72e81d7e4664a9801593f98d1c5, 2963192),
    ('zksync', 'main', 0x78e30497a3c7527d953c6B1E3541b021A98Ac43c, 44671759),
    ('ethereum', 'prime', 0x4e033931ad43597d96D6bcc25c280717730B58B1, 20262414),
    ('ethereum', 'ether.fi', 0x0AA97c284e98396202b6A04024F5E2c65026F3c0, 20625519),
    ('ethereum', 'horizon', 0xAe05Cd22df81871bc7cC2a04BeCfb516bFe332C8, 23125535)

)
, reserve_market_filtered as (
    SELECT blockchain, contract_address, block_number
    FROM reserve_markets
    WHERE blockchain = '{{chain}}' and instance = '{{instance}}'
)
, reserve_data as (
    SELECT date_trunc('day', block_time) as time
        , bytearray_substring(topic1, 1 + 12, 20) as reserve
        , reserve_market_filtered.blockchain
        , POWER(1 + max_by(bytearray_to_int256(bytearray_substring(data, 1, 32)) * 1e-27, block_time)/31536000, 31536000) - 1 as supply_rate
        , MAX(bytearray_to_int256(bytearray_substring(data, 1 + 32 * 3, 32)) * 1e-27) as liquidity_index
        , MAX(bytearray_to_int256(bytearray_substring(data, 1 + 32 * 4, 32)) * 1e-27) as borrow_index
        , POWER(1 + max_by(bytearray_to_int256(bytearray_substring(data, 1 + 32 * 2, 32)) * 1e-27, block_time)/31536000, 31536000) - 1 as borrow_rate
    FROM evms.logs as evm_logs
        JOIN reserve_market_filtered on evm_logs.contract_address = reserve_market_filtered.contract_address
            and reserve_market_filtered.blockchain = evm_logs.blockchain
    WHERE topic0 = 0x804c9b842b2748a22bb64b345453a3de7ca54a6ca45ce00d415894979e22897a
        and evm_logs.block_number >= reserve_market_filtered.block_number
    GROUP BY 1, 2, 3
)
, reserve_metrics as (

    SELECT series_data.day
        , symbol, asset, series_data.blockchain
        , COALESCE(supply_rate, LAST_VALUE(supply_rate) IGNORE NULLS OVER (PARTITION BY series_data.asset ORDER BY series_data.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) as supply_rate
        , COALESCE(liquidity_index, LAST_VALUE(liquidity_index) IGNORE NULLS OVER (PARTITION BY series_data.asset ORDER BY series_data.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) as liquidity_index
        , COALESCE(borrow_index, LAST_VALUE(borrow_index) IGNORE NULLS OVER (PARTITION BY series_data.asset ORDER BY series_data.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) as borrow_index
        , COALESCE(borrow_rate, LAST_VALUE(borrow_rate) IGNORE NULLS OVER (PARTITION BY series_data.asset ORDER BY series_data.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) as borrow_rate
    FROM
        series_data
        LEFT JOIN reserve_data on series_data.day = reserve_data.time and series_data.asset = reserve_data.reserve
)
, sdai_txs as (
    SELECT date_trunc('day', call_block_time) as day
        , max_by(output_tmp * 1e-27, call_block_time) as price_per_share
    FROM maker_ethereum.pot_call_drip
    WHERE call_block_number >= 8928300 -- 2019-11-13 19:53
    GROUP BY 1
)
, sdai_pricing as (
    SELECT day, 0x83F20F44975D03b1b09e64809B757c47f942BEeA as token_address, 'sDAI' as symbol, AVG(price) as price
    FROM (
        SELECT
          days.day,
          COALESCE(sdai.price_per_share, LAG(sdai.price_per_share, 1) OVER (ORDER BY days.day)) as price
        FROM days LEFT JOIN sdai_txs as sdai ON sdai.day = days.day
    )
    WHERE day >= TIMESTAMP'2022-07-23 00:00'
    GROUP BY 1
)
, pricing as (
    select dt, token_address, tokens.symbol, price_usd
    from dune.steakhouse.result_token_price as all_pricing
        JOIN tokens on all_pricing.token_address = tokens.asset
            and all_pricing.blockchain = tokens.blockchain
    where dt >= DATE(tokens.creation_time) and all_pricing.blockchain = '{{chain}}'
    UNION ALL
    SELECT day, token_address, symbol, price
    FROM sdai_pricing
)
, tvl as (
    SELECT series_data.symbol, series_data.day
        , series_data.asset
        , series_data.aToken, series_data.vdToken
        , pricing.price_usd
        , SUM(total_supply.supply) as deposit_tokens
        , SUM(total_debt.debt) as borrow_tokens
        , SUM(total_debt.debt_change) * pricing.price_usd as debt_change
        , SUM(total_supply.supply_change) * pricing.price_usd as supply_change
        , SUM(total_supply.supply_change) as supply_change_tokens
    FROM series_data
        LEFT JOIN total_supply on series_data.day = total_supply.day and series_data.symbol = total_supply.symbol
            and series_data.asset = total_supply.asset
        LEFT JOIN total_debt on series_data.day = total_debt.day and series_data.symbol = total_debt.symbol
            and series_data.asset = total_debt.asset
        LEFT JOIN pricing on series_data.day = pricing.dt and series_data.symbol = pricing.symbol
            and series_data.asset = pricing.token_address
    GROUP BY 1, 2, 3, 4, 5, pricing.price_usd
)
, tvl_collaterals as (
    SELECT day, ARRAY_AGG(DISTINCT symbol ORDER BY symbol) as collaterals
    FROM tvl
    group by 1
)
select
    CASE
        when tvl.asset IN (
            0xff970a61a04b1ca14834a43f5de4533ebddb5cc8   -- Arbitrum
            , 0x7f5c764cbc14f9669b88837ca1490cca17c31607 -- Optimism
            , 0x2791bca1f2de4661ed88a30c99a7a9449aa84174 -- Polygon
        ) THEN 'USDC.e' 
        when tvl.asset in (
            0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca  -- Base   
        ) THEN 'USDbC'
        ELSE UPPER(tvl.symbol)
    END as symbol
    , tvl.day as period 
    , blockchain, 'aave' as protocol, '3' as version
    , '{{instance}}' as instance
    , aToken, vdToken
    , tvl_collaterals.collaterals -- only main ones
    , CASE WHEN supply_change = 0 then null else supply_change end as supply_delta_usd
    , CASE WHEN supply_change_tokens = 0 then null else supply_change_tokens end as delta_tokens
    , CASE WHEN debt_change = 0 then null else debt_change end as borrow_delta_usd
    , case when deposit_tokens = 0 then null else liquidity_index * price_usd * deposit_tokens end as supply_usd
    , case when deposit_tokens = 0 then null else liquidity_index * deposit_tokens end as supply_tokens
    , case when borrow_tokens = 0 then null else borrow_index * price_usd * borrow_tokens end as borrow_usd
    , case when borrow_tokens = 0 then null else borrow_index * borrow_tokens end as borrow_tokens
    , case when deposit_tokens + borrow_tokens = 0 then null else price_usd * (liquidity_index * deposit_tokens + borrow_index * borrow_tokens) end as total_usd
    , price_usd * borrow_tokens * IF(deposit_tokens=0, null, borrow_index * borrow_tokens / (liquidity_index * deposit_tokens)) * borrow_rate / 365 as interest_paid_usd
    , liquidity_index as supply_index, borrow_index
    , CASE WHEN tvl.day != current_date then (liquidity_index / LAG(liquidity_index) OVER (PARTITION BY blockchain, tvl.asset, aToken ORDER BY tvl.day) - 1) * 365 else supply_rate end as supply_rate
    , CASE WHEN tvl.day != current_date then (borrow_index / LAG(borrow_index) OVER (PARTITION BY blockchain, tvl.asset, vdToken ORDER BY tvl.day) - 1) * 365 else borrow_rate end as borrow_rate
    , IF(deposit_tokens=0, null, borrow_index * borrow_tokens / (liquidity_index * deposit_tokens)) as utilization_ratio
FROM tvl
    LEFT JOIN reserve_metrics on tvl.day = reserve_metrics.day
    and tvl.asset = reserve_metrics.asset 
    and tvl.symbol = reserve_metrics.symbol
    JOIN tvl_collaterals on tvl.day = tvl_collaterals.day
ORDER BY tvl.day DESC, tvl.symbol 