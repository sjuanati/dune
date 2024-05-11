/*
kpis:
- total market cap (ie: 130B)
- dominance per asset (ie: USDT 43%)
- dominance per category (ie: crypto-backed: 80% )
- dominance per chain

*/

/*
SELECT 'ethereum', category, sum("Market Cap") as "Market Cap" FROM query_3581067 GROUP BY 1, 2
UNION ALL
SELECT 'arbitrum', category, sum("Market Cap") as "Market Cap" FROM query_3679025 GROUP BY 1, 2
UNION ALL
SELECT 'polygon', category, sum("Market Cap") as "Market Cap" FROM query_3679021 GROUP BY 1, 2
UNION ALL
SELECT 'xxxxxx', category, sum("Market Cap") as "Market Cap" FROM query_xxxxx GROUP BY 1, 2
UNION ALL
SELECT 'xxxxxx', category, sum("Market Cap") as "Market Cap" FROM query_xxxxx GROUP BY 1, 2
UNION ALL
SELECT 'xxxxxx', category, sum("Market Cap") as "Market Cap" FROM query_xxxxx GROUP BY 1, 2
UNION ALL
SELECT 'xxxxxx', category, sum("Market Cap") as "Market Cap" FROM query_xxxxx GROUP BY 1, 2
UNION ALL
SELECT 'xxxxxx', category, sum("Market Cap") as "Market Cap" FROM query_xxxxx GROUP BY 1, 2
UNION ALL
SELECT 'xxxxxx', category, sum("Market Cap") as "Market Cap" FROM query_xxxxx GROUP BY 1, 2
UNION ALL
SELECT 'xxxxxx', category, sum("Market Cap") as "Market Cap" FROM query_xxxxx GROUP BY 1, 2
UNION ALL
SELECT 'xxxxxx', category, sum("Market Cap") as "Market Cap" FROM query_xxxxx GROUP BY 1, 2
UNION ALL
SELECT 'xxxxxx', category, sum("Market Cap") as "Market Cap" FROM query_xxxxx GROUP BY 1, 2
*/

-- get only the latest date of all queries

with
    data as (
        select 'arbitrum' as chain, 'algorithmic' as category, 36817746 as "Market Cap"
        union all
        select 'arbitrum' as chain, 'crypto-backed' as category, 114987667 as "Market Cap"
        union all
        select 'arbitrum' as chain, 'fiat-backed' as category, 3441450565 as "Market Cap"
        union all
        select 'polygon' as chain, 'algorithmic' as category, 3374057 as "Market Cap"
        union all
        select 'polygon' as chain, 'fiat-backed' as category, 1400585676 as "Market Cap"
        union all
        select 'polygon' as chain, 'crypto-backed' as category, 183597753 as "Market Cap"
    ),
    totals as (
        select
            chain,
            category,
            "Market Cap",
            sum("Market Cap") over () as "Total",
            100.0 * sum("Market Cap") over (partition by chain) / sum("Market Cap") over () as "Total per chain",
            100.0 * sum("Market Cap") over (partition by category) / sum("Market Cap") over () as "Total per cat"
        from data
    )

select * from totals
