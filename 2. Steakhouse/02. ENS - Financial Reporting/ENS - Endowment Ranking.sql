/*
-- @title: ENS - Endowment Ranking
-- @description: Gives a snapshot of the latest ENS endowment composition in USD value, excluding 0 value assets.
-- @author: Steakhouse Financial
-- @notes: N/A
-- @version:
    - 1.0 - 2024-02-19 - Initial version
*/

select item, period, balance
from query_2840308 -- ENS - Endowment
where period = (select MAX(period) from query_2840308)
  and balance > 1
order by balance desc
