/*
-- @title: Interest Accruals
-- @author: Steakhouse Financial
-- @description: provides only the debt & rate updates from vaults, but the interest rate (cost of borrowing)
                is calculated in other queries by accumulating the debt per ilk and multiplying the debt by the rate
-- @notes: N/A
-- @version:
    - 1.0 - 2024-05-22 - Initial version
    - 2.0 - 2024-10-21 - Resolve the debt calculation for the LITE-PSM txs.
    - 3.0 - 2024-10-30 - Remove references to LITE-PSM txs. 
*/

with interest_accruals as (
        -- frob: update debt in vault
        --       dart > 0 -> user creates more dai (debt) against their collateral
        --       dart < 0 -> user pays back dai (debt) against their collateral
        --       dart = 0 -> collateral is changed (dink) but debt is not changed (dart). Excluded from this query
        select
            i as ilk,
            call_block_time as ts,
            call_tx_hash as hash,
            dart,
            null as rate,
            call_trace_address
        from maker_ethereum.vat_call_frob
        where call_success
        and dart != 0 -- 

        union all
        -- grab: update debt in vault during liquidation
        -- @dev: not clear when dart > 0 -> why is this increasing debt instead of keeping/reducing it?
        --       dart > 0 -> system records an increase in bad debt
        --       dart < 0 -> system clears or reduces the outstanding dai (debt) as collateral is liquidated 
        select
            i as ilk,
            call_block_time ts,
            call_tx_hash hash,
            dart,
            0 as rate,
            call_trace_address
        from maker_ethereum.vat_call_grab
        where call_success
        and dart != 0
        union all
        -- fold: adjust the stability fee accumulation rate for a specific type of collateral
        --       interest rate (cost of borrowing) determines how much Dai debt accumulates over time as a stability fee
        select
            i as ilk,
            call_block_time ts,
            call_tx_hash hash,
            null as dart,
            rate,
            call_trace_address
        from maker_ethereum.vat_call_fold
        where call_success
        and rate != 0
    )

select * from interest_accruals