/*
-- @title: Maker - Revenues Composition - Current v2
-- @author: Steakhouse Financial
-- @description: Calculates annualized revenues by collateral type as of the latest data refresh
-- @notes: N/A
-- @version:
    - 1.0 - 2024-06-04 - Initial version
*/

select *
from query_3700249 -- Maker - Assets per type
where date(dt) = current_date