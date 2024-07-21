/*
-- @title: Maker - RWA Current
-- @author: Steakhouse Financial
-- @description: categorizes real-world assets by collateral type and calculates the total outstanding debt
                 for each category as of the latest data refresh
-- @notes: this query requires manual update if new RWA-based ilks are created (query_3690739)
-- @version:
    - 1.0 - 2024-06-06 - Initial version
*/

with
    rwa_categories as (
        select
            dt,
            case
                when ilk like 'RWA007%' then 'Monetalis (Clydesdale)'
                when ilk like 'RWA009%' then 'H.V. Bank'
                when ilk like 'RWA012%' then 'BlockTower S3'
                when ilk like 'RWA013%' then 'BlockTower S4'
                when ilk like 'RWA014%' then 'Coinbase'
                when ilk like 'RWA015%' then 'BlockTower Andromeda'
                else 'Other'
            end as collateral,
            total_debt
        from query_3690739 -- Maker - RWA v2
    ),
    rwa as (
        select
            dt,
            collateral,
            total_debt,
            max(dt) over () as latest_day
        from rwa_categories
        where dt > current_date - interval '2' month
    )

select
    dt,
    collateral,
    sum(total_debt) as total_debt,
    sum(total_debt / 1e6) as total_debt_m
from rwa
where dt = latest_day
group by 1, 2
order by 1 desc