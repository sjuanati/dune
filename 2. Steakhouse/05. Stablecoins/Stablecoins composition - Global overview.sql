with
    data as (
        select asset_name as asset, 'ethereum' as chain, category, sum("Market Cap") as market_cap
        from dune.wint3rmute.result_stablecoins_composition_ethereum_overview
        group by 1, 2, 3
        union all
        select asset_name, 'arbitrum', category, sum("Market Cap")
        from dune.wint3rmute.result_stablecoins_composition_arbitrum_overview
        group by 1, 2, 3
        union all
        select asset_name, 'polygon', category, sum("Market Cap")
        from dune.wint3rmute.result_stablecoins_composition_polygon_overview
        group by 1, 2, 3
        union all
        select asset_name, 'bsc', category, sum("Market Cap")
        from dune.wint3rmute.result_stablecoins_composition_bsc_overview
        group by 1, 2, 3
        union all
        select asset_name, 'avalanche', category, sum("Market Cap")
        from dune.wint3rmute.result_stablecoins_composition_avalanche_overview
        group by 1, 2, 3
        union all
        select asset_name, 'base', category, sum("Market Cap")
        from dune.wint3rmute.result_stablecoins_composition_base_overview
        group by 1, 2, 3
        union all
        select asset_name, 'optimism', category, sum("Market Cap")
        from dune.wint3rmute.result_stablecoins_composition_optimism_overview
        group by 1, 2, 3
        union all
        select asset_name, 'tron', category, sum("Market Cap")
        from dune.wint3rmute.result_stablecoins_composition_tron_overview
        group by 1, 2, 3
        union all
        select asset_name, 'solana', category, sum("Market Cap")
        from dune.wint3rmute.result_stablecoins_composition_solana_overview
        group by 1, 2, 3
    ),
    totals as (
        select
            asset,
            chain,
            category,
            market_cap,
            sum(market_cap / 1e9)  over () as "Total"
            --sum(market_cap) over (partition by chain) / sum(market_cap) over () as "Total x Chain",
            --sum(market_cap) over (partition by category) / sum(market_cap) over () as "Total x Category",
            --sum(market_cap) over (partition by asset) / sum(market_cap) over () as "Total x Asset"
        from data
    )

select * from totals
