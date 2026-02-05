WITH curve_tokens(contract_address, symbol, token0_address, token1_address) as (
    VALUES
    (0xA632D59b9B804a956BfaA9b48Af3A1b74808FC1f, 'PYUSDUSDS', 0x6c3ea9036406852006290770bedfcaba0e23a0e8, 0xdC035D45d973E3EC169d2276DDab16f1e407384F),
    (0x00836Fe54625BE242BcFA286207795405ca4fD10, 'sUSDSUSDT', 0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD, 0xdAC17F958D2ee523a2206206994597C13D831ec7)
)
, tokens as (
    SELECT token_address, symbol, decimals
    FROM dune.steakhouse.result_token_info
    where blockchain = 'ethereum'
    and token_address in (
        0xdac17f958d2ee523a2206206994597c13d831ec7 -- USDT
        , 0xdC035D45d973E3EC169d2276DDab16f1e407384F -- USDS
        , 0x6c3ea9036406852006290770bedfcaba0e23a0e8 -- PYUSD
        , 0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD -- sUSDS (Ethereum)
    )
)
, token_prices as (
    select dt, token_address, symbol, COALESCE(accounting_price_usd, price_usd) as price
    FROM dune.steakhouse.result_token_price as p
        JOIN tokens as t using (token_address, symbol)
    where p.blockchain = 'ethereum'
)
, date_series as (

    SELECT dt, contract_address, symbol
    FROM (
        SELECT contract_address, symbol FROM curve_tokens
        UNION ALL
        SELECT token_address, symbol FROM tokens
    ), unnest(sequence(
        TIMESTAMP'2025-04-07 00:00',
        current_date,
        interval '1' day)
    ) as s(dt)
)
, pool_dates as (
    SELECT dt, p.pool_address, token_address as contract_address, t.symbol
    FROM (
        select contract_address as pool_address, token0_address as token_address from curve_tokens
        union all
        select contract_address, token1_address from curve_tokens
    ) as p LEFT JOIN tokens as t using(token_address)
    , unnest(sequence(
        TIMESTAMP'2025-04-07 00:00',
        current_date,
        interval '1' day)
    ) as s(dt)
)
, token_supply as (

    SELECT date(ts) as dt, contract_address as pool_address, symbol, SUM(value) as value
    FROM (
        SELECT evt_block_time as ts, contract_address, symbol, (value/1e18) as value
        FROM erc20_ethereum.evt_transfer
            JOIN curve_tokens using (contract_address)
        where "from" = 0x0000000000000000000000000000000000000000
        UNION ALL
        SELECT evt_block_time as ts, contract_address, symbol, -(value/1e18) as value
        FROM erc20_ethereum.evt_transfer
            JOIN curve_tokens using (contract_address)
        where "to" = 0x0000000000000000000000000000000000000000
    )
    GROUP BY 1, 2, 3

)
, series_tokens as (
    -- select dt, pool_address, token_address, symbol, total_supply, price * total_supply as underlying_usd 
    -- from (
    
        SELECT dt, pool_address, SUM(value) OVER (PARTITION BY pool_address order by dt) as tokens_supply
        FROM (
            select dt, pool_address
            from pool_dates
            group by 1, 2
        ) left join token_supply using (dt, pool_address)

    -- ) as activity join token_prices using (dt, token_address, symbol)

)
, underlying_supply as (
    SELECT date(ts) as dt, pool_address, contract_address, symbol, SUM(value) as value
    FROM (
        SELECT t.evt_block_time as ts, ct.contract_address as pool_address, t.contract_address, tok.symbol, (value/POWER(10, tok.decimals)) as value
        FROM erc20_ethereum.evt_transfer as t
            JOIN curve_tokens as ct on t."to" = ct.contract_address and t.contract_address in (ct.token0_address, ct.token1_address)
            LEFT JOIN tokens as tok on t.contract_address = tok.token_address
        UNION ALL
        SELECT t.evt_block_time as ts, ct.contract_address as pool_address, t.contract_address, tok.symbol, -(value/POWER(10, tok.decimals)) as value
        FROM erc20_ethereum.evt_transfer as t
            JOIN curve_tokens as ct on t."from" = ct.contract_address and t.contract_address in (ct.token0_address, ct.token1_address)
            LEFT JOIN tokens as tok on t.contract_address = tok.token_address
    )
    GROUP BY 1, 2, 3, 4
)
, series_underlying as (
    select dt, pool_address, token_address, symbol, total_supply, price * total_supply as underlying_usd 
    from (
    
        SELECT dt, pool_address, contract_address as token_address, symbol, SUM(value) OVER (PARTITION BY contract_address, pool_address order by dt) as total_supply
        FROM pool_dates left join underlying_supply using (dt, pool_address, symbol, contract_address)

    ) as activity join token_prices using (dt, token_address, symbol)
)
, series as (

    SELECT dt, pool_address, total_usd / tokens_supply as lp_price, LAG(total_usd / tokens_supply) OVER (PARTITION BY pool_address ORDER BY dt) as last_lp_price
    FROM (
        SELECT dt, pool_address, SUM(underlying_usd) as total_usd
        FROM series_underlying GROUP BY 1, 2
    )
    JOIN series_tokens using (dt, pool_address)
    WHERE dt >= TIMESTAMP'2025-04-24 00:00'
)

SELECT dt, pool_address, lp_price, last_lp_price
, CASE WHEN apy < 0 then 0 ELSE apy end apy
FROM (
    SELECT dt, pool_address, lp_price, last_lp_price
    , 365 * ((lp_price/last_lp_price) - 1) as apy 
    FROM series
)