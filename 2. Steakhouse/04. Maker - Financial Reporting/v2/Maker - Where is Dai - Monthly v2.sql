/*
-- @title: Maker - Where is Dai - Monthly
-- @author: Steakhouse Financial
-- @description: Visualizes the monthly distribution of DAI across various usage categories since Jun'20
-- @notes: N/A
-- @version:
    - 1.0 - 2024-06-18 - Initial version
*/

with
    dai as (
        select
            *,
            max(dt) over (partition by year(dt), month(dt)) as last_day_of_month
        from query_892721 -- where is day
    )

select
    dt,
    wallet,
    balance
from dai
where dt = last_day_of_month
order by 1 desc