/*
@title: Morpho V2 - Markets
@description: shows all Morpho markets created, incl. market name, loan/coll asset & decimals
@author: Steakhouse Financial
@dev: n/a
@version:
    - 1.0 - 2025-03-14 - Initial version
    - 2.0 - 2025-04-11 - Added formatting for tokens, linking with their native chain
    - 3.0 - 2025-05-08 - Added Chain Explorer wrapper functions in case statements for loan, coll, and tx hash
    - 4.0 - 2025-05-10 - Add instance to the final result
    - 5.0 - 2025-07-16 - Add in Unichain and Katana links for markets
*/

with
    markets as (
        select
            evt_tx_hash as tx_hash,
            c.evt_block_time as ts,
            c.chain as blockchain,
            c.id as market_id,
            from_hex(lpad(json_query(c.marketParams, 'lax $.loanToken' omit quotes), 42, '0')) as loan_address,
            from_hex(lpad(json_query(c.marketParams, 'lax $.collateralToken' omit quotes), 42, '0')) as coll_address,
            cast(json_query(c.marketParams, 'lax $.lltv' omit quotes) as int256) / POWER(10, 18) as lltv
        from morpho_blue_multichain.morphoblue_evt_createmarket c
    ),
    -- add info to markets: loan symbol & name, collateral symbol & name, and market symbol & name
    markets_extended as (
        select
            *,
            format('%s/%s', coll_symbol, loan_symbol) as market_name,
            format('%s/%s-%.0f%%-%d', coll_symbol, loan_symbol, lltv * 100, rank() over (partition by coll_symbol || '/' || loan_symbol order by market_id)) as market_symbol
        from (
            select
                m.tx_hash,
                m.ts,
                m.blockchain,
                m.market_id,
                m.loan_address,
                if(m.loan_address = 0x0000000000000000000000000000000000000000, 'idle', coalesce(l.symbol, 'NA')) as loan_symbol,
                coalesce(l.decimals, 18) as loan_decimals,
                m.coll_address,
                if(m.coll_address = 0x0000000000000000000000000000000000000000, 'idle', coalesce(c.symbol, 'NA')) as coll_symbol,
                coalesce(c.decimals, 18) as coll_decimals,
                m.lltv
            from markets m
            left join dune.steakhouse.result_token_info l -- info for loan token
                on m.loan_address = l.token_address
                and m.blockchain = l.blockchain
            left join dune.steakhouse.result_token_info c -- info for collateral token
                on m.coll_address = c.token_address
                and m.blockchain = c.blockchain
        )
    ),
    -- adding fancy links to txns, tokens & markets
    markets_formatted as (
        select
            blockchain,
            concat(
                format('%1$TY-%1$Tm-%1$Td %1$TH:%1$TM', ts),
                ' ',
                CASE WHEN blockchain = 'katana'
                    THEN get_href(format('https://ww4.katanascan.com/tx/%s', tx_hash), '🔗')
                    ELSE get_href(get_chain_explorer_tx_hash(blockchain, tx_hash), '🔗')
                END
            ) as creation_ts,
            concat(
                market_name, ' ',
                get_href(
                CASE 
                    WHEN blockchain in ('ethereum', 'base', 'unichain', 'katana') 
                        THEN format('https://legacy.morpho.org/market?id=%s&network=%s', market_id, IF(blockchain = 'ethereum', 'mainnet', blockchain))
                    WHEN blockchain = 'polygon' 
                        THEN format('https://www.compound.blue/borrow/%s', market_id)
                    WHEN blockchain in ('worldchain', 'corn') 
                        THEN format('https://oku.trade/morpho/markets?inputChain=%s&selectedMarket=%s', blockchain, market_id)
                    -- Links are not as easy
                    -- WHEN blockchain = 'optimism' 
                        -- THEN FORMAT('https://moonwell.fi/markets/supply/optimism/%s', blockchain, vault_symbol)
                    -- Doesn't work always
                    ELSE format('https://legacy.morpho.org/market?id=%s&network=%s', market_id, IF(blockchain = 'ethereum', 'mainnet', blockchain))
                    -- Not launched
                END, '🔗')
            ) as market,
            lltv,
            concat(
                loan_symbol,
                ' ',
                CASE WHEN blockchain = 'katana'
                    THEN get_href(format('https://ww4.katanascan.com/address/%s', loan_address), '🔗')
                    ELSE get_href(get_chain_explorer_address(blockchain, loan_address), '🔗')
                END
            ) as loan_token,
            concat(
                coll_symbol,
                ' ',
                CASE WHEN blockchain = 'katana'
                    THEN get_href(format('https://ww4.katanascan.com/address/%s', coll_address), '🔗')
                    ELSE get_href(get_chain_explorer_address(blockchain, coll_address), '🔗')
                END
            ) as coll_token,
            date(ts) as creation_dt,
            market_id,
            market_name,
            market_symbol,
            loan_address,
            loan_symbol,
            loan_decimals,
            coll_address,
            coll_symbol,
            coll_decimals,
            CASE 
                WHEN blockchain IN ('ethereum', 'base') THEN 'main'
                ELSE 'lite'
            END as instance
        from markets_extended
    )

select * from markets_formatted order by creation_ts desc