/*
-- @title: Core - Trades-based price
-- @description: Handles prices from dex.trades that are not found or fully synched with dex.prices
-- @author: Steakhouse Financial
-- @notes: when adding new prices, remember updating matview query_4583090
-- @version:
    - 1.0 - 2025-01-17 - Initial version
    - 2.0 - 2025-02-20 - Add in USDz
    - 3.0 - 2025-03-06 - Add in rUSD
                        -> Excluded wM inaccurate txs
    - 4.0 - 2025-03-14 - Pull fixed_priced_tokens from our token info dataset
    - 5.0 - 2025-03-26 - tokens cte refactored to avoid dependency with dune.steakhouse.result_token_info
    - 6.0 - 2025-05-25 - Removed union with dex.prices
*/

with
    tokens (blockchain, start_date, token_address, decimals, symbol, price_address) as (
        values
            ('base', date '2024-08-27', 0x0a27e060c0406f8ab7b64e3bee036a37e5a62853, 18, 'xUSDz', null),
            ('ethereum', date '2024-08-19', 0x09d4214c03d01f49544c0448dbe3a27f768f2b34, 18, 'rUSD', 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),
            ('ethereum', date '2024-07-29', 0x69000405f9dce69bd4cbf4f2865b79144a69bfe0, 18, 'USDz', null),
            ('ethereum', date '2024-08-14', 0x437cc33344a0b27a429f795ff6b469c72698b291, 6, 'wM', null),
            ('base', date'2025-01-08', 0x6bb7a212910682dcfdbd5bcbb3e28fb4e8da10ee, 18, 'GHO', null),
            ('ink', date'2025-06-28', 0xfc421ad3c883bf9e7c4f42de845c4e4405799e73, 18, 'GHO', null),
            ('avalanche_c', date'2025-06-28', 0xfc421ad3c883bf9e7c4f42de845c4e4405799e73, 18, 'GHO', null),
            ('base', date'2023-06-25', 0x236aa50979D5f3De3Bd1Eeb40E81137F22ab794b, 18, 'tBTC', null),
            ('bnb', date'2020-11-22', 0x4bd17003473389a42daf6a0a729f6fdb328bbbd7, 18, 'VAI', null)
    ),
    trades as (
        select
            date(dt.block_time) as dt,
            t.blockchain,
            t.symbol as symbol,
            t.token_address,
            max_by(dt.amount_usd / if(t.token_address = dt.token_bought_address, dt.token_bought_amount, dt.token_sold_amount), dt.block_time) as price_usd
        from dex.trades dt
        join tokens t
            on t.token_address in (dt.token_bought_address, dt.token_sold_address)
            and dt.blockchain = t.blockchain
            and DATE(dt.block_time) >= t.start_date
            and price_address is null
        where amount_usd is not null
        and tx_hash not in (
            -- wM transactions with issues
            0xc3f70057e261af554c6acf6a372389899f0c2d7d1ebd27311e39525dee88fb39,
            0xeb1d80622e6bf059cd3f023b42ff2c80e9cdd4d9e44ab1f9808da1746318709a
        )
        group by 1, 2, 3, 4
    )

select * from trades order by 1 desc, 4 asc