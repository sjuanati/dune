/*
-- @title: ENS - Endowment
-- @description: Calculates the daily balance of the ENS Endowment for the latest month closed
-- @author: Steakhouse Financial
-- @notes: N/A
-- @version:
    - 1.0 - 2024-03-15 - Initial version
*/

with
    amounts as (
        select
            period,
            item,
            balance
        from query_2840308-- ENS - Endowment
        where date_trunc('month', period) = date_trunc('month', current_date) - interval '1' month -- last closed month
          and balance > 1
    ),
    latest_amounts as (
        select
            sum(case when item like '%ETH%' then balance end) / 1000000 as total_eth,
            sum(case when item not like '%ETH%' then balance end) / 1000000 as total_stablecoin,
            sum(balance) / 1000000 as total_both
        from amounts
        where period = (select max(period) from amounts)
    )

select * from amounts, latest_amounts