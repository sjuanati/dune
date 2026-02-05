/*
-- @title: Token Price
-- @description: Provides the daily price in USD for a long list of tokens!
-- @author: Steakhouse Financial
-- @ER model: https://docs.google.com/presentation/d/1JI2b12FBZaV0nInBFB2rbvQiXHBdylDHc1Kq3oJlA8M
-- @version:
    - 1.0 - 2025-02-06 - Initial version
    - 2.0 - 2025-03-27 - Getting tokens from result_token_info only if they have a price feed
    - 3.0 - 2025-03-28 - Added price safety check (discard dex price when > 2M USD)
    - 4.0 - 2025-05-06 - Price history starting from the first price feed per token (instead of global start date for all tokens)
                       - Added calculated_price_usd, default_price_usd & dex_price_usd
                       - Added EUR to USD conversion for eur-stablecoins
    - 5.0 - 2025-05-13 - Added morpho calcs into <calculated_pricing> cte
    - 6.0 - 2025-05-14 - Removed 2 price outliers from <default_pricing> cte
    - 7.0 - 2025-05-22 - Using prices.day instead of prices.usd_daily & dex.prices + backfill backwards missing stablecoin prices
*/
-- @todo: remove matview 'dune.steakhouse.result_lending_protocols_asset_info' -- Lending Protocols - Asset info

with
    -- list of tokens to retrieve prices for
    tokens as (
        select
            blockchain,
            symbol,
            token_address,
            underlying_address as price_address,
            date(start_date) as start_date,
            "type"
        from dune.steakhouse.result_token_info
        where has_price
    ),
    -- create a sequence for all tokens since their creation date
    series as (
        select s.dt, t.*
        from tokens t
        cross join unnest(sequence(t.start_date, current_date, interval '1' day)) as s(dt)
    ),
    -- find the price through alternative calculations for assets when price does not exist in prices.day
    calculated_pricing as (
        select
            dt,
            blockchain,
            symbol,
            token_address,
            price_usd 
        from dune.steakhouse.result_token_alternative_price
        where token_address is not null -- exclude EUR price
        union all
        select
            dt,
            blockchain,
            symbol,
            token_address,
            price_usd
        from dune.steakhouse.result_core_morpho_price
    ),
    -- find the price from dune table prices.day (coinpaprika + dex.trades)
    dune_pricing as (
        select
            date(d."timestamp") as dt,
            t.blockchain,
            t.symbol,
            t.token_address,
            min_by(price, source) as price_usd
        from prices.day d
        join tokens t
            on d.contract_address = coalesce(t.price_address, t.token_address)
            and d.blockchain = t.blockchain
        where d."timestamp" >= t.start_date
          and d.price < 2e6 -- avoid crazy prices too off (to be updated when BTC to the moon)
        group by 1,2,3,4
    ),
    -- find the EUR to USD conversion (based on chainlink feeds)
    euro_pricing as (
        select
            dt,
            price_usd
        from dune.steakhouse.result_token_alternative_price
        where token_address is null
          and symbol = 'EUR'
          and blockchain = 'ethereum'
    ),
    -- backfill forward days with missing prices
    pricing_backfill as (
        select
            dt,
            blockchain,
            s.symbol,
            s."type",
            token_address,
            coalesce(
                cp.price_usd,
                last_value(cp.price_usd) ignore nulls over (partition by blockchain, token_address order by dt rows between unbounded preceding and current row)
            )  as calculated_price_usd,
            coalesce(
                dp.price_usd,
                last_value(dp.price_usd) ignore nulls over (partition by blockchain, token_address order by dt rows between unbounded preceding and current row)
            )  as dune_price_usd,
            case
                when s."type" = 'usd-stablecoin' then 1
                when s."type" = 'eur-stablecoin' then eu.price_usd
                when s."type" = 'stablecoin' then 1 -- @dev: RISKY: could be non usd-stablecoins. Request to Dune to include <currency> in erc20_stablecoins.
            end * 1e0 as accounting_price_usd
        from series s
        left join calculated_pricing cp using (dt, blockchain, token_address)
        left join dune_pricing dp using (dt, blockchain, token_address)
        left join euro_pricing eu using (dt)
    ),
    -- get the first non-null price for stablecoins to backfill the gap backwards between the token creation date and the first price feed
    first_price_stablecoins as (
        select
            blockchain,
            token_address,
            min(dt) as first_price_dt,
            min_by(coalesce(calculated_price_usd, dune_price_usd, accounting_price_usd), dt) as first_price_usd
        from pricing_backfill
        where coalesce(calculated_price_usd, dune_price_usd, accounting_price_usd) is not null
          and "type" like '%stablecoin%'
        group by 1,2
    ),
    -- choose final price_usd by order of priority
    pricing as (
        select
            p.dt,
            blockchain,
            p.symbol,
            token_address,
            p.calculated_price_usd,
            p.dune_price_usd,
            coalesce(
                p.calculated_price_usd,
                p.dune_price_usd,
                case
                    when p."type" like '%stablecoin%' and fp.first_price_usd is not null and p.dt < fp.first_price_dt
                        then fp.first_price_usd
                    else p.accounting_price_usd
                end
            ) as price_usd,
            p.accounting_price_usd
        from pricing_backfill p
        left join first_price_stablecoins fp
            using (blockchain, token_address)
    )
    
select *
from pricing
order by dt desc, blockchain asc, lower(symbol) asc