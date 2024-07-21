/*
-- @title: Maker - Revenues Composition - Monthly v2
-- @author: Steakhouse Financial
-- @description: Calculates annualized revenues by collateral type over the last rolling 13 months
-- @notes: N/A
-- @version:
    - 1.0 - 2024-06-18 - Initial version
*/

with
    revenues as (
        select
            dt,
            collateral,
            asset,
            annual_revenues,
            max(dt) over (partition by year(dt), month(dt)) as last_day_of_month -- including current month
        from query_3700249 -- Maker - Assets per type
        where dt > current_date - interval '13' month
    )

select *
from revenues
where  dt = last_day_of_month