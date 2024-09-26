/*
-- @title: Badger eBTC - Overview
-- @author: Steakhouse Financial
-- @description: Provides debt, collateral, protocol yield share, open positions, unique addresses
--               and different collateral ratios per CDP position aggregated at daily level
-- @notes: N/A
-- @version:
    - 1.0 - 2024-08-30 - Initial version
*/

with
    oracle_ebtc as (
        select 
            date(evt_block_time) as dt,
            price as oracle_price
        from (
            select 
                evt_block_time,
                _lastGoodPrice / 1e18 as price,
                row_number() over (partition by date(evt_block_time) order by evt_block_time desc) as rn
            from badgerdao_ethereum.PriceFeed_evt_LastGoodPriceUpdated
        ) ranked
        where rn = 1
        order by 1 desc
    ),
    -- final aggregated metrics by date
    cdp_totals as (
        select
            dt,
            oracle_price,
            sum(cdp.debt) as debt,
            sum(cdp.coll) as coll,
            sum(if(cdp.is_active, 1, 0)) as active_cdps,
            count(distinct(borrower)) as unique_addr,
            sum(cdp.coll_pys) as coll_pys,
            sum(cdp.pys_acc) as pys,
            sum(pys_acc_usd) as pys_usd,
            sum(cdp.debt_usd) as debt_usd,
            sum(cdp.coll_usd) as coll_usd,
            sum(cdp.coll_pys_usd) as coll_pys_usd,
            sum(cdp.coll_pys_usd) / sum(debt_usd) as tcr,
            (sum(cdp.coll_pys_usd) / sum(debt_usd)) * 100 as tcr_per,
            1.25 as ccr, -- @todo: fixed or tracked through event?
            1.1 as mcr
        from query_4040136 cdp
        left join oracle_ebtc ora using (dt)
        group by 1, 2
    )

select * from cdp_totals order by dt desc