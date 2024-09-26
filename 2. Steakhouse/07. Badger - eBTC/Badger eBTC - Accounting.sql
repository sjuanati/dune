/*
-- @title: Badger eBTC - Accounting
-- @author: Steakhouse Financial
-- @description: Generates double-entry accounting for Badger's eBTC, which serves as the basis for calculating the profit & loss and balance sheet
-- @notes:
        @todo: no events yet on: 1) stETH flashloans 2)redemptions
        @dev: using <cast(null as varchar) as customer> to be able to create a materialized view (can't infer type from an always null value)
-- @version:
        1.0 - 2024-09-04 - Initial version
*/

with
    debt as (
        /************************************************************************************************
        ******************************************** D E B T ********************************************
        *************************************************************************************************/
        select
            'DEBT' as ledger,
            u.evt_block_time as ts,
            u.evt_tx_hash as txn_hash,
            u.evt_index,
            u._borrower as wallet_addr,
            0x661c70333AA1850CcDBAe82776Bb436A0fCfeEfB as token_addr, -- eBTC
            concat('cdp:', cast(varbinary_substring(u._cdpId, 29, 4) as varchar), ', op:', cast(u._operation as varchar)) as metadata,
            array[1, 1] as sign,
            array[
                '1020101', -- Inv. Crypto-Backed Loans
                '20101'    -- Liab. Non-yielding
            ] as account_id,
            coalesce((u._debt / 1e18 - u._oldDebt / 1e18), 0) as amount_txn,
            coalesce((u._debt / 1e18 - u._oldDebt / 1e18), 0) as amount_base,
            coalesce((u._debt / 1e18 - u._oldDebt / 1e18), 0) * c.price_btc as value_usd
        from badgerdao_ethereum.CdpManager_evt_CdpUpdated u
        left join query_4042988 c on date(u.evt_block_time) = c.dt -- Conversions
        where u._operation in (0, 1, 2, 4) -- 0: open CDP, 1: close CDP, 2: adjust CDP, 4: Normal liquidation
        and u._debt != u._oldDebt -- exclude collateral updates not affecting debt
        order by 1, 2, 3
    ),
    rev_flash_loans as (
        /************************************************************************************************
        *********************** R E V E N U E S  :  F L A S H   L O A N   F E E S ***********************
        *************************************************************************************************/
        -- @dev: flash loans can be done with eBTC or stETH
        select
            'REV' as ledger,
            ts,
            txn_hash,
            evt_index,
            wallet_addr,
            token_addr,
            'FLASH-LOANS' as metadata, -- naming convention TBC
            array[1, 1] as sign,
            array[
                if(
                    token_addr = 0x661c70333AA1850CcDBAe82776Bb436A0fCfeEfB, -- eBTC
                    '1010101', -- Liq. Non-yielding [BC] -> eBTC
                    '1010202'  -- Liq. Yielding [Non-BC] -> stETH
                ),
                '3010101'  -- Rev. Platform Fees
            ] as account_id,
            fee as amount_txn,
            if(token_addr = 0x661c70333AA1850CcDBAe82776Bb436A0fCfeEfB, fee, fee * (price_steth / price_btc)) as amount_base,
            if(token_addr = 0x661c70333AA1850CcDBAe82776Bb436A0fCfeEfB, fee * price_btc, fee * price_steth) as value_usd
        from (
            select
                f.evt_block_time as ts,
                f.evt_tx_hash as txn_hash,
                f.evt_index,
                f._receiver as wallet_addr,
                f._token as token_addr,
                f._fee / 1e18 as fee,
                c.price_btc,
                c.price_steth
            from ebtc_ethereum.BorrowerOperations_evt_FlashLoanSuccess f
            left join query_4042988 c on date(f.evt_block_time) = c.dt -- Conversions
        )
        order by 1, 2, 3
    ),
    rev_pys as (
        /************************************************************************************************
        ****************************** R E V E N U E S  :  P Y S   F E E S ******************************
        *************************************************************************************************/
        -- @dev: using lag() in order to decumulate data and show only incremental amounts, because the
        --       balance sheet calculations are again accumulating data.
        select
            'REV' as ledger,
            date_trunc('month', dt) as ts,
            null as txn_hash,
            0 as evt_index,
            null as wallet_addr,
            0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84 as token_addr,
            'PYS' as metadata, -- naming convention TBC
            array[1, 1] as sign,
            array[
                '1010202', -- Liq. Yielding [Non-BC]
                '3010101'  -- Rev. Platform Fees
            ] as account_id,
            p.pys - lag(p.pys, 1, 0) over (order by dt asc) as amount_txn,
            (p.pys - lag(p.pys, 1, 0) over (order by dt asc)) * (c.price_steth / c.price_btc) as amount_base,
            --p.pys_usd - lag(p.pys_usd, 1, 0) over (order by dt asc) as value_usd -> nope! price variance biases the lag() window
            (p.pys - lag(p.pys, 1, 0) over (order by dt asc)) * (c.price_steth / c.price_btc) * c.price_btc as value_usd
        from query_4025994 p -- eBTC Overview
        left join query_4042988 c using (dt) -- Conversions
        where dt = last_day_of_month(dt) or dt = current_date -- monthly basis
        order by 1, 2
    ),
    rev_redemption as (
        select 1
        /************************************************************************************************
        *********************** R E V E N U E S  :  R E D E M P T I O N   F E E S ***********************
        *************************************************************************************************/
        -- @dev: No redemption events emitted yet
        /*
        union all
        event Redemption(
            uint256 _debtToRedeemExpected,
            uint256 _debtToRedeemActual,
            uint256 _collSharesSent,
            uint256 _feeCollShares,
            address indexed _redeemer
        );
        */
    ),
        /************************************************************************************************
        **************************** E X P E N S E S  : I N C E N T I V E S *****************************
        *************************************************************************************************/
    merkle_new_campaigns as (
        select
            from_hex(json_extract_scalar(campaign, '$.campaignId')) as campaignId,
            from_hex(json_extract_scalar(campaign, '$.creator')) as creator,
            from_hex(json_extract_scalar(campaign, '$.rewardToken')) as rewardToken,
            cast(json_extract_scalar(campaign, '$.amount') as uint256) as amount,
            cast(json_extract_scalar(campaign, '$.campaignType') as integer) as campaignType,
            cast(json_extract_scalar(campaign, '$.startTimestamp') as integer) as startTimestamp,
            cast(json_extract_scalar(campaign, '$.duration') as integer) as duration
        from merkl_ethereum.DistributionCreator_evt_NewCampaign
    ),
    ebtc_campaigns as (
        select
            campaignId,
            rewardToken,
            from_unixtime(startTimestamp) as startDate,
            from_unixtime(startTimestamp + duration) as endDate,
            date_diff(
                'day',
                from_unixtime(startTimestamp),
                from_unixtime(startTimestamp + duration)
            ) as days,
            amount / 1e18 as amount,
            sequence(
                date(from_unixtime(startTimestamp)),
                date(from_unixtime(startTimestamp + duration)),
                interval '1' day
            ) as seq
        from merkle_new_campaigns
        where creator = 0xb76782b51bff9c27ba69c77027e20abd92bcf3a8   -- probably Badger multisig
        and rewardToken = 0x3472A5A71965499acd81997a54BBA8D852C6E53d -- BADGER
        and startTimestamp > 1724803200 -- 28.08.2024
        and campaignType = 9 -- Borrow
    ),
    exp_incentives as (
        select
            'EXP' as ledger,
            s.dt as ts,
            null as txn_hash,
            null as evt_index,
            null as wallet_addr,
            ca.rewardToken as token_addr,
            'INCENTIVES' as metadata, -- naming convention TBC
            array[1, -1] as sign,
            array[
                '3040203', -- Contra Equity
                '3020104'  -- Exp. Incentives
            ] as account_id,
            (amount / days) as amount_txn,
            (amount / days) * (co.price_badger / co.price_btc) as amount_base,
            (amount / days) * co.price_badger as value_usd
        from ebtc_campaigns ca
        cross join unnest(seq) as s(dt)
        left join query_4042988 co on s.dt = co.dt -- Conversions
        where s.dt <= current_date
    ),
    -- groups all accounting entries
    accounting as (
        select * from debt
        union all
        select * from rev_flash_loans
        union all
        select * from rev_pys
        union all
        select * from exp_incentives
    ),
    -- generates double-entry records for each accounting entry
    dual as (
        select
            ledger || '-' || cast(rank() over (order by ts, txn_hash, evt_index) as varchar) as entry,
            1 as line_num,
            account_id[1] as account_id,
            ledger,
            ts,
            'BADGER' as entity,
            'EBTC' as business_line,
            cast(null as varchar) as customer,
            1 as chain_id,
            txn_hash,
            token_addr,
            wallet_addr,
            metadata,
            sign[1] * amount_txn as amount_txn,
            sign[1] * amount_base as amount_base,
            sign[1] * value_usd as value_usd
        from accounting
        union all
        select
            ledger || '-' || cast(rank() over (order by ts, txn_hash, evt_index) as varchar) as entry,
            2 as line_num,
            account_id[2] as account_id,
            ledger,
            ts,
            'BADGER' as entity,
            'EBTC' as business_line,
            cast(null as varchar) as customer,
            1 as chain_id,
            txn_hash,
            token_addr,
            wallet_addr,
            metadata,
            sign[2] * amount_txn as amount_txn,
            sign[2] * amount_base as amount_base,
            sign[2] * value_usd as value_usd
        from accounting
    )

select * from dual order by ts asc