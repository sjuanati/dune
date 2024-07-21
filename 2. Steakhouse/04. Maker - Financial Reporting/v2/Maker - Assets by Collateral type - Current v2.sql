/*
-- @title: Maker - Assets by Collateral type - Current v2
-- @author: Steakhouse Financial
-- @description: Calculates debt, revenue, and annualized revenues by collateral type as of the latest data refresh
-- @notes: N/A
-- @version:
    - 1.0 - 2024-06-06 - Initial version
*/

with
    assets as (
        select
            dt,
            collateral,
            asset
        from query_3700249 -- Assets per type (daily)
    ),
    latest_day as (
        select max(dt) as dt from assets
    )

select *
from assets
join latest_day using (dt)