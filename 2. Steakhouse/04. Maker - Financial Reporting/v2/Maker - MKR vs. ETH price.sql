/*
-- @title: MKR vs ETH Price
-- @description: Provides the lastest MKR price in USD and daily price action of MKR & ETH since 2020
-- @author: Steakhouse Financial
-- @notes: N/A
-- @version:
    - 1.0 - 2023-06-05 - Initial version
    - 2.0 - 2024-05-07 - Added comment header, added time filter in prices.usd
    - 3.0 - 2024-06-20 - Added ETH price
*/

with
    tokens (contract_address, ticker) as (
        values
            (0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2, 'MKR'), -- mkr
            (0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 'ETH')  -- eth
    ),
    latest_mkr_update as (
        select max(minute) as latest_minute
        from prices.usd p
        where date(p.minute) >= date '2024-06-20'
        and blockchain = 'ethereum'
        and contract_address = 0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2 --mkr
    ),
    -- ensure eth latest price isn't more recent than mkr, as the mkr latest price counter
    -- requires that the very last record in the query result is mkr and not eth
    latest_eth_update as (
        select max(minute) as latest_minute
        from prices.usd p,
             latest_mkr_update mkr
        where date(p.minute) >= date '2024-06-20'
        and blockchain = 'ethereum'
        and contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 --eth
        and p.minute <= mkr.latest_minute
    ),
    mkr_eth_prices as (
        select
            t.ticker as token,
            minute,
            price
        from prices.usd p
        join tokens t on p.contract_address = t.contract_address
        cross join latest_mkr_update mkr
        cross join latest_eth_update eth
        where date(minute) >= date '2020-01-01'
        and blockchain = 'ethereum'
        and (extract(minute from p.minute) = 59 and extract(hour from p.minute) = 23)
        or (
            (t.ticker = 'MKR' and p.minute = mkr.latest_minute) 
            or
            (t.ticker = 'ETH' and p.minute = eth.latest_minute)
        )
    )

select * from mkr_eth_prices order by minute desc, token desc