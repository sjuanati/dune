/*
-- @title: Maker - Assets by Collateral type v2
-- @author: Steakhouse Financial
-- @description: Calculates debt, revenue, and annualized revenues by collateral type over the last rolling 13 months
-- @notes: N/A
-- @version:
    - 1.0 - 2024-06-06 - Initial version
*/

with
    assets as (
        select
            dt,
            collateral,
            asset,
            total_asset,
            max(dt) over (partition by year(dt), month(dt)) as last_day_of_month
        from query_3700249 -- Assets per type (daily)
        where dt > current_date - interval '13' month
    )

select
    dt,
    collateral,
    asset,
    total_asset
from assets
where dt = last_day_of_month
order by dt desc