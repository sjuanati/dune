/*
-- @title: Badger eBTC - CDP Details
-- @author: Steakhouse Financial
-- @description: Provides debt, collateral, protocol yield share and different collateral ratios per CDP position
-- @notes:
--       - field <is_active> to be validated depending on non-tested statuses -> currently using operations 0, 2, 3 & 7
--       - ignore CDPs with 0 debt & coll?
-- @version:
    - 1.0 - 2024-09-03 - Initial version
*/

with
    -- retrieves debt and collateral from all cdp operations
    -- using rn to get only the latest operation x day
    cdp_ops as (
        select
            date(u.evt_block_time) as dt,
            varbinary_to_uint256(varbinary_substring(_cdpId, 29, 4)) AS cdp_number,
            _operation as op,
            case
                when _operation in (0, 2, 3, 7) then true -- @todo: TBC
                else false 
            end as is_active,
            conv.factor,
            u._borrower as borrower,
            coalesce(u._debt / 1e18, 0) as debt,
            case
                when i1._newIndex is not null then coalesce((u._collShares / 1e18) * (i1._newIndex / 1e18), 0)
                when conv.index is not null then coalesce((u._collShares / 1e18) * conv.index, 0)
                else -1
            end as coll,
            row_number() over (partition by date(u.evt_block_time), _cdpId order by u.evt_block_time desc) as rn
        from badgerdao_ethereum.CdpManager_evt_CdpUpdated u
        left join badgerdao_ethereum.CdpManager_evt_StEthIndexUpdated i1 using(evt_tx_hash)
        left join query_4042988 conv on date(u.evt_block_time) = conv.dt -- Conversions
        where u._operation in (0, 1, 2, 4) -- 0: open CDP, 1: close CDP, 2: adjust CDP, 4: Normal liquidation
        order by 1 asc
    ),
    -- gets the date of the first operation (ie: open) per cdp
    cdp_mindate as (
        select
            cdp_number,
            min(dt) as min_dt
        from cdp_ops
        group by 1
    ),
    -- date filling for every cdp from its cdp open date until the present day,
    -- carrying forward the debt, collateral and factor
    cdp_filling as (
        select
            dates.dt as dt,
            md.cdp_number,
            cdp.op,
            last_value(is_active) ignore nulls over (partition by md.cdp_number order by dates.dt asc) as is_active,
            last_value(cdp.borrower) ignore nulls over (partition by md.cdp_number order by dates.dt asc) as borrower,
            cdp.coll as new_coll,
            last_value(factor) ignore nulls over (partition by md.cdp_number order by dates.dt asc) as start_factor,
            last_value(cdp.debt) ignore nulls over (partition by md.cdp_number order by dates.dt asc) as debt,
            last_value(cdp.coll) ignore nulls over (partition by md.cdp_number order by dates.dt asc) as coll
        from cdp_mindate md
        cross join lateral (select dt from unnest(sequence(md.min_dt, current_date, interval '1' day)) as t(dt)) as dates
        left join cdp_ops cdp
           on dates.dt = cdp.dt
           and cdp.rn = 1
           and md.cdp_number = cdp.cdp_number
    ),
    -- stores factor on every cdp change, because the collateral amount (stEth) is updated with the latest yield; therefore,
    -- we restart the factor on every cdp change to be able to calculate all future yield accrued until the next update
    cdp_accruals as (
        select
            dt,
            cdp_number,
            op,
            is_active,
            borrower,
            debt,
            cdp.coll as coll_original,
            c.factor,
            cdp.start_factor,
            (cdp.coll * (c.factor / cdp.start_factor)) as coll,
            lag(cdp.coll * (c.factor / cdp.start_factor)) over (partition by cdp_number order by dt) as coll_prev
        from cdp_filling cdp
        left join query_4042988 c using (dt) -- Conversions
    ),
    -- provides the Protocol Yield Share (PYS) to be applied over a period (aka. staking reward split), starting at 50%
    pys_fee as (
        select
            time_series.dt as dt,
            last_value(srs._stakingRewardSplit / 1e4) ignore nulls over (order by time_series.dt asc) as fee
        from (select dt from unnest(sequence(date '2024-03-15', current_date, interval '1' day)) as t(dt)) as time_series
        left join badgerdao_ethereum.CdpManager_evt_StakingRewardSplitSet srs
            on time_series.dt = date(srs.evt_block_time)
    ),
    -- calculates the PYS to be deducted from each CDP collateral: ((100% - PYS) * stETH's yield)
    cdp_pys as (
        select
            *,
            sum(c.coll - if(c.op is null, c.coll_prev, c.coll)) over (partition by c.cdp_number order by dt) * coalesce(pys.fee, 0) as pys_acc,
            c.coll - if(c.op is null, c.coll - c.coll_original, 0) * coalesce(pys.fee, 0) as coll_pys
        from cdp_accruals c
        left join pys_fee pys using(dt)
    ),
    -- provides all relevant indicators per CDP position
    cdp_final as (
        select
            dt,
            c.cdp_number,
            c.op,
            c.is_active,
            c.borrower,
            c.debt,
            c.coll_original,
            c.coll,
            c.coll_pys,
            c.debt * conv.price_btc as debt_usd,
            c.coll * conv.price_steth as coll_usd,
            c.coll_pys * conv.price_steth as coll_pys_usd,
            c.pys_acc,
            c.pys_acc * conv.price_steth as pys_acc_usd,
            if(c.debt = 0, 0, (c.coll_pys * conv.price_steth) / (c.debt * conv.price_btc)) as icr,
            1.25 as ccr,
            1.10 as mcr
        from cdp_pys c
        left join query_4042988 conv using (dt) -- Conversions
    )

select * from cdp_final