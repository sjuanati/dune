-- @todo: header
-- @goal: Alternative way to calculate price when it is missing in dune's tables <prices.day>
with
    alternative_pricing as (
        select dt, blockchain, symbol, price_address, token_address, share_price, price_usd
        from query_4532733 -- Core - Shares-based price [query_5030027 -- test]
        union all
        select dt, blockchain, symbol, null as price_address, token_address, null as share_price, price_usd
        from query_4534624 -- Core - Oracle-based price [query_5032500 -- test]
        union all
        select dt, blockchain, symbol, null as price_address, token_address, null as share_price, price_usd
        from query_4565771 -- Core - Trades-based price
        union all
        select dt, blockchain, symbol, price_address, token_address, share_price, price_usd
        from query_4568025 -- Core - Pendle tokens price [query_5084521 -- test]
        union all
        select dt, blockchain, symbol, null as price_address, token_address, null as share_price, price_usd
        from query_4516512 -- Core - Others' price [query_5036876 -- test]
    )

select
    dt,
    blockchain,
    symbol,
    token_address,
    price_address,
    share_price,
    price_usd
from alternative_pricing
