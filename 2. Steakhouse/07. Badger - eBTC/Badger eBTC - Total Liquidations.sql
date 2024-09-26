/*
-- @title: Badger eBTC - Total Liquidations
-- @author: Steakhouse Financial
-- @description: shows the stETH amount of active collateral (open CDPs) vs. the stETH amount of liquidated collateral
-- @notes: N/A
-- @version:
        1.0 - 2024-09-04 - Initial version
*/


with
    coll_total as (
        select
            'Active' as "type",
            coll_pys as coll
        from query_4025994 -- Overview
        where dt = current_date
        union all
        select *
        from (
            select
                'Liquidated' as "type",
                coll_liq_agg as coll
            from query_4042931 -- Liquidations
            limit 1
        )
    )

select * from coll_total