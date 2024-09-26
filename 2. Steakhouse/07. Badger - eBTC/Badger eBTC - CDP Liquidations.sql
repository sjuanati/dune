/*
-- @title: Badger eBTC - CDP Liquidations
-- @author: Steakhouse Financial
-- @description: shows data for all CDP positions liquidated
-- @notes: N/A
-- @version:
        1.0 - 2024-09-04 - Initial version
*/

with
    liquidations as (
        select
            l.evt_block_time as ts,
            concat(
                '<a href="https://etherscan.io/tx/',
                cast(l.evt_tx_hash as varchar),
                '" target="_blank" > 🔗 </a>'
            ) as tx,
            concat(
                '<a href="https://debank.com/profile/',
                cast(l._borrower as varchar),
                '" target="_blank" >',
                "left"(cast(l._borrower as varchar), 6),
                '...',
                "right"(cast(l._borrower as varchar), 4),
                '</a>'
            ) as borrower,
            varbinary_to_uint256(varbinary_substring(l._cdpId, 29, 4)) AS cdp_number,
            (l._collShares / 1e18) * coalesce(conv.index, 0) as coll,
            l._debt / 1e18 as debt,
            (l._collShares / 1e18) * coalesce(conv.index, 0) * conv.price_steth as coll_usd,
            (l._debt / 1e18) * conv.price_btc as debt_usd,
            l._liquidator as liquidator,
            concat(
                '<a href="https://debank.com/profile/',
                cast(l._liquidator as varchar),
                '" target="_blank" >',
                "left"(cast(l._liquidator as varchar), 6),
                '...',
                "right"(cast(l._liquidator as varchar), 4),
                '</a>'
            ) as liquidator,
            l._premiumToLiquidator / 1e18 as premium_to_liquidator
        from badgerdao_ethereum.CdpManager_evt_CdpLiquidated l
        left join query_4042988 conv on date(l.evt_block_time) = conv.dt
    ),
    liquidations_agg as (
        select
            sum(coll) as coll_liq_agg,
            sum(coll_usd) as coll_liq_usd_agg
        from liquidations
    )
    
select * from liquidations, liquidations_agg order by ts asc