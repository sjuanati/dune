/*
-- @title: Core - Pendle tokens price
-- @description: Handles prices derived from shares prices for Pendle Tokens
-- @author: Steakhouse Financial
-- @notes: when adding new prices, remember updating matview query_4583090
-- @version:
    - 1.0 - 2025-01-15 - Initial version
    - 2.0 - 2025-01-16 - Add new tokens
    - 3.0 - 2025-03-06 - Utilise token info dataset and modify to new iteration
    - 4.0 - 2025-03-12 - Add header comments
    - 5.0 - 2025-03-26 - Replaced dune.steakhouse.result_token_info by dune.steakhouse.dataset_token_info_inclusions to
                         avoid circular dependency with info tokens
    - 6.0 - 2025-05-05 - Code refactory, removed unused fields
    - 7.0 - 2025-05-25 - Replaced dex.prices by prices.day
    - 8.0 - 2026-01-28 - Add multichain support
    - 9.0 - 2026-01-30 - Temporarily add Arbitrum MarketDeployment contract address
*/

with
    principal_dataset as (
        select symbol as principal_symbol
            , blockchain
            , underlying_address as asset_address
            , token_address as pt_address
            , decimals as principal_decimals
        from dune.steakhouse.dataset_token_info_inclusions
        where "type" = 'pendle'
    ),
    markets_deployed as (
        select evt_block_date as dt
            , chain as blockchain
            , market
            , sy as sy_address
            , pt as pt_address
            , evt_tx_hash
        from pendle_multichain.pendlepooldeployhelper_evt_marketdeployment
        union all
        select evt_block_date as dt
            , 'ethereum' as blockchain
            , market
            , sy as sy_address
            , pt as pt_address
            , evt_tx_hash
        from pendle_ethereum.pendlepooldeployhelper2_evt_marketdeployment
        UNION ALL
        SELECT block_date, 'arbitrum' as blockchain
            , bytearray_substring(data, 1 + 12 + 32 * 3, 20) as market
            , bytearray_substring(data, 1 + 12 + 32 * 0, 20) as sy_address
            , bytearray_substring(data, 1 + 12 + 32 * 1, 20) as pt_address
            , tx_hash
        FROM arbitrum.logs
        WHERE contract_address = 0x2Ed473F528E5B320f850d17ADfe0e558f0298aA9 -- Pendle CommonDeployer
        and topic0 = 0xd1f8866e1ab220ea57cc2bc3d029810357a6f6df863760170473f9df5b322ebd -- MarketDeployment
        UNION ALL
        select evt_block_date
            , 'hyperevm' as blockchain
            , FROM_HEX(json_extract_scalar(addrs, '$.SY')) AS sy_address
            , FROM_HEX(json_extract_scalar(addrs, '$.PT')) AS pt_address
            , FROM_HEX(json_extract_scalar(addrs, '$.market')) AS market_address
            , evt_tx_hash
        FROM pendle_hyperevm.pendlecommonpooldeployhelperv2_evt_marketdeployment
    ),
    market_creation as (
        select blockchain
            , m.market
            , m.sy_address
            , d.principal_symbol as name
            , pt_address
            , d.asset_address
            , d.principal_decimals
            , m.dt as creation_ts
        from markets_deployed m
        join principal_dataset d using (blockchain, pt_address)
    ),
    date_series as (
        select s.dt
            , m.blockchain
            , m.pt_address as token_address
            , m.name as symbol
            , m.market
        from unnest(sequence(date '2024-01-01', current_date, interval '1' day)) as s(dt)
        join market_creation m
            on s.dt >= m.creation_ts
    ),
    pt_swaps as (
        select evt_block_time as ts,
               chain as blockchain,
               market,
               token as contract_address,
               abs(netpttoaccount) as pt_amount,
               abs(nettokentoaccount) as token_amount,
               evt_tx_hash as tx_hash
        from pendle_multichain.routerv4_evt_swapptandtoken  -- original: routerv4_evt_SwapPtAndToken
        where evt_block_time >= timestamp '2024-04-29 12:10'
    ),
    market_swap as (
        select ts,
               blockchain,
               m.name,
               market,
               symbol,
               contract_address,
               s.pt_amount * power(10, -m.principal_decimals) as pt_amount,
               s.token_amount * power(10, -t.decimals) as token_amount,
               s.tx_hash
        from pt_swaps s
        join tokens.erc20 t using (contract_address, blockchain)
        join market_creation m using (blockchain, market)
    ),
    dex_pricing as (
        select date(d."timestamp") as dt,
               contract_address,
               min_by(d.price, source) as price
        from prices.day d
        join (select distinct contract_address, blockchain from market_swap) t
            using (blockchain, contract_address)
        where d."timestamp" >= timestamp '2024-01-01 00:00:00'
        group by 1, 2
    ),
    pt_pricing as (
        select date(ts) as dt,
               blockchain,
               market,
               name,
               max_by(pt_price, ts) as pt_price
        from (
            select s.ts,
                   blockchain,
                   s.market,
                   s.symbol,
                   s.name,
                   s.pt_amount,
                   dx.price * s.token_amount / s.pt_amount as pt_price,
                   dx.price,
                   s.tx_hash
            from market_swap s
            join dex_pricing dx
              on dx.dt = date(s.ts)
             and s.contract_address = dx.contract_address
             where pt_amount > 0
        )
        group by 1, 2, 3, 4
    )

select
    s.dt,
    s.blockchain,
    s.symbol,
    try_cast(null as varbinary) as price_address,
    s.token_address,
    try_cast(null as double) as share_price,
    coalesce(
       pt_price,
       last_value(pt_price) ignore nulls over (partition by s.market order by s.dt)
    ) as price_usd
from date_series s
left join pt_pricing p
    on p.dt = s.dt
    and p.market = s.market
order by s.dt desc
