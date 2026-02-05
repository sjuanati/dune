/*
-- @title: SKY Interest Accruals + DSR
-- @author: Steakhouse Financial
-- @version:
    - 1.0 - 2024-05-27 - Initial version
    - 2.0 - 2024-09-29 - Filter out UNI V2 LP for Operating Expenses
    - 3.0 - 2024-10-29 - Migrate changes for LITE-PSM introduction
                        -> Added in efficiency in pulling in Interest Accruals in one query.
                        -> Made exclusions to Spark USDs contract for sin outflows.
    - 4.0 - 2024-10-30 - Add configuration for Lite PSM flows
    - 5.0 - 2024-11-02 - Track flows for PSM 
    - 6.0 - 2024-11-02 - Add in specifications for PSM configuration 
    - 7.0 - 2024-11-04 - Exclude DAI from the LITE-PSM both as an asset and liability
    - 8.0 - 2024-11-06 - Merge new changes to query
    - 9.0 - 2025-05-24 - Add loan actions plugs
    - 10.0 - 2025-05-25 - Add idle spDAI flows
                        -> Add idle Morpho idle flows
                        -> Add descriptors for idle amounts
    - 11.0 - 2025-05-26 - Merge new changes to query
    - 12.0 - 2025-05-31 - Minor update comments
    - 13.0 - 2025-11-12 - Add idle amounts for PYUSDUSDS and spUSDS
    - 14.0 - 2025-11-19 - Add Star Revenues into results
    - 15.0 - 2026-01-06 - Remove stUSDS and Kicker contract from sin flows
*/


with
    -- ***************************************************************************
    -- ************************* I D L E   A M O U N T S *************************
    -- ***************************************************************************
    idle_lite_psm_flows as (
        SELECT evt_block_time as ts
            , 0x4c4954452d50534d2d555344432d41 as ilk -- LITE-PSM-USDC-A
            , (CASE WHEN dst = 0xf6e72db5454dd049d0788e411b06cfaf16853042 THEN 1 ELSE -1 END * CAST(wad AS INT256)) as value 
            , evt_tx_hash as hash
        from maker_ethereum.dai_evt_transfer
        WHERE contract_address = 0x6b175474e89094c44da98b954eedeac495271d0f -- DAI
        and evt_block_number >= 20421763
        and 0xf6e72db5454dd049d0788e411b06cfaf16853042 in (dst, src) -- LITE PSM USDC A
    )
    , idle_spark_flows as (
        SELECT evt_block_time as ts
            , NULL as ilk
            , (CASE WHEN dst = 0x4DEDf26112B3Ec8eC46e7E31EA5e123490B05B8B THEN 1 ELSE -1 END * CAST(wad AS INT256)) as value 
            , evt_tx_hash as hash
        from maker_ethereum.dai_evt_transfer
        WHERE contract_address = 0x6b175474e89094c44da98b954eedeac495271d0f -- DAI
        and evt_block_number >= 16932378
        and 0x4DEDf26112B3Ec8eC46e7E31EA5e123490B05B8B in (dst, src) -- Idle spDAI
    )
    , idle_spark_usds_flows as (
        SELECT evt_block_time as ts
            , NULL as ilk
            , (CASE WHEN "to" = 0xC02aB1A5eaA8d1B114EF786D9bde108cD4364359 THEN 1 ELSE -1 END * CAST(value AS INT256)) as value 
            , evt_tx_hash as hash
        from sky_ethereum.usds_evt_transfer
        WHERE contract_address = 0xdC035D45d973E3EC169d2276DDab16f1e407384F -- USDS
        and evt_block_number >= 21723481
        and 0xC02aB1A5eaA8d1B114EF786D9bde108cD4364359 in ("from", "to") -- Idle spUSDS
        UNION ALL
        SELECT evt_block_time as ts
            , NULL as ilk
            , (CASE WHEN "to" = 0xA632D59b9B804a956BfaA9b48Af3A1b74808FC1f THEN 1 ELSE -1 END * CAST(value AS INT256)) as value 
            , evt_tx_hash as hash
        from sky_ethereum.usds_evt_transfer
        WHERE contract_address = 0xdC035D45d973E3EC169d2276DDab16f1e407384F -- USDS
        and evt_block_number >= 23301123
        and 0xA632D59b9B804a956BfaA9b48Af3A1b74808FC1f in ("from", "to") -- Idle PYUSDUSDS 

    )
    , idle_morpho_flows as (
        -- https://legacy.morpho.org/market?id=0x57f4e42c0707d3ae0ae39c9343dcba78ff79fa663da040eca45717a9b0b0557f&network=mainnet
        select evt_block_time as ts, NULL as ilk, assets as value, evt_tx_hash as hash
        from morpho_blue_ethereum.morphoblue_evt_supply
        WHERE id = 0x57f4e42c0707d3ae0ae39c9343dcba78ff79fa663da040eca45717a9b0b0557f -- Idle [DAI]
        UNION ALL
        select evt_block_time as ts, NULL as ilk, -assets as value, evt_tx_hash as hash
        from morpho_blue_ethereum.morphoblue_evt_withdraw
        WHERE id = 0x57f4e42c0707d3ae0ae39c9343dcba78ff79fa663da040eca45717a9b0b0557f -- Idle [DAI]
    )
    , idle_sum as (
        SELECT ts
            , hash
            , from_utf8(bytearray_rtrim(ilk)) as ilk
            , 'Idle Lite PSM Flows' as descriptor
            , SUM(value / POWER(10, 18)) as value
        FROM idle_lite_psm_flows
        GROUP BY 1, 2, 3
        UNION ALL
        SELECT ts
            , hash
            , ilk
            , 'Idle spDAI Flows' as descriptor
            , SUM(value / POWER(10, 18)) as value
        FROM idle_spark_flows
        GROUP BY 1, 2, 3
        UNION ALL
        SELECT ts
            , hash
            , ilk
            , 'Idle Morpho Flows' as descriptor
            , SUM(value / POWER(10, 18)) as value
        FROM idle_morpho_flows
        GROUP BY 1, 2, 3
    )
    , idle_usds_sum as (
        
        SELECT ts
            , hash
            , ilk
            , 'Idle USDS' as descriptor
            , SUM(value / POWER(10, 18)) as value
        FROM idle_spark_usds_flows
        group by 1, 2, 3

    )
    -- Those transaction decrease the PSM/spDAI asset by preminted DAI and decrease the DAI circulation by the same amount
    -- Therefore, the amount of preminted DAI is not affecting the balance sheet
    , idle_txs as (
        select
            ts,
            hash,
            descriptor,
            211201 as account_id, -- Preminted DAI (Liabilities)
            -value as value,
            ilk
        from idle_sum
        UNION ALL
        select
            ts,
            hash,
            descriptor,
            134111 as account_id, -- Preminted DAI (Assets)
            -value as value,
            ilk
        from idle_sum
        UNION ALL
        select
            ts,
            hash,
            descriptor,
            211211 as account_id, -- Preminted USDS (Liabilities)
            -value as value,
            ilk
        from idle_usds_sum
        UNION ALL
        select
            ts,
            hash,
            descriptor,
            134112 as account_id, -- Preminted USDS (Assets)
            -value as value,
            ilk
        from idle_usds_sum
    )
    -- ***************************************************************************
    -- ******************** I N T E R E S T   A C C R U A L S ********************
    -- ***************************************************************************
    , interest_accruals_txs as (
        SELECT ilk
            , ts
            , hash
            , dart
            , rate
            , call_trace_address
        FROM query_3754893 -- Maker - Interest Accruals v2
    )
    , accruals_cum as (
        select
            ts,
            hash,
            ilk,
            rate,
            call_trace_address,
            sum(dart) over (partition by ilk order by ts asc, call_trace_address asc) as cumulative_dart
        from interest_accruals_txs -- Maker - Interest Accruals v2
    ),
    accruals_sum as (
        select
            ts,
            hash,
            from_utf8(bytearray_rtrim(ilk)) as ilk,
            sum(cumulative_dart * rate) * 1e-45 as interest_accruals
        from accruals_cum
        where rate is not null
        group by 1, 2, 3
    ),
    main_interest_accruals as (
        select
            acc.ts,
            acc.hash,
            acc.ilk,
            col.equity_account_id as account_id,
            sum(acc.interest_accruals) as value -- increased equity
        from accruals_sum acc
        left join query_3685400 col -- Sky - Collaterals - Detailed v2
            on acc.ilk = col.ilk
            and DATE(acc.ts) between col."start" and col."end"
        group by 1, 2, 3, 4
        union all
        select
            acc.ts,
            acc.hash,
            acc.ilk,
            col.asset_account_id as account_id,
            sum(acc.interest_accruals) as value -- increased assets
        from accruals_sum acc
        left join query_3685400 col -- Sky - Collaterals - Detailed v2
            on acc.ilk = col.ilk
            and DATE(acc.ts) between col."start" and col."end"
        group by 1, 2, 3, 4
    ),
    -- ***************************************************************************
    -- ********************* L E N D I N G   R E V E N U E S *********************
    -- ***************************************************************************
    cum_rates as (
        select
            date(ts) as ts,
            hash,
            from_utf8(bytearray_rtrim(ilk)) as ilk,
            dart,
            coalesce(
                1e27 + sum(rate) over (partition by ilk order by ts asc, call_trace_address asc),
                1e27
            ) as rate
        from interest_accruals_txs -- Maker - Interest Accruals v2
        where from_utf8(bytearray_rtrim(ilk)) != 'TELEPORT-FW-A'
    ),
    loan_actions_preunioned as (
        select
            cr.ts,
            cr.hash,
            cr.ilk,
            col.asset_account_id as account_id,
            sum((cr.dart * cr.rate) / POWER(10, 45)) as value
        from cum_rates cr
        left join query_3685400 col -- Sky - Collaterals - Detailed v2
            on cr.ilk = col.ilk
            and date(cr.ts) between col."start" and col."end"
        group by 1, 2, 3, 4
        having 1e-45 * sum(dart * rate) != 0
    ),
    loan_actions as (
        -- Assets
        -- increased assets
        SELECT ts
            , hash
            , ilk
            , account_id
            , value
        FROM loan_actions_preunioned
        UNION ALL
        -- Liabilities
        -- increased liabilities
        SELECT ts
            , hash
            , ilk
            , 21120 as account_id -- dai (non-interest bearing)
            , value
        FROM loan_actions_preunioned
    ),
    loan_actions_plugged as (
        -- https://forum.sky.money/t/consolfreight-rwa-003-cf4-drop-default/21745/21
        ---> Following a default of RWA003-A DAI was irrecoverable
        ---> The Protocol absorbed the bad debt
        select
            ts,
            hash,
            ilk,
            31740 as account_id, -- Direct to Third Party Expenses
            value -- increased assets
        from loan_actions_preunioned
        WHERE hash = 0x789c9271fdb91b6afcc48386ea7bd15bb2928ddb347817a922988160380c72be
        and ilk = 'RWA003-A'
        UNION ALL
        select
            ts,
            hash,
            ilk,
            21120 as account_id,  -- dai (non-interest bearing)
            -value -- decreased liabilities
        from loan_actions_preunioned cr
        WHERE hash = 0x789c9271fdb91b6afcc48386ea7bd15bb2928ddb347817a922988160380c72be
        and ilk = 'RWA003-A'
    )
    -- ***************************************************************************
    -- ************************ S T A R   R E V E N U E S ************************
    -- ***************************************************************************
    -- https://forum.sky.money/t/msc-3-settlemnt-summary-october-2025-initial-calculation/27397/2?u=shogun
    -- , star_revenues_debt as (
    --     -- Liquidations of the Allocator Vaults are used to realise distribute revenues to SKY from Star
    --     SELECT call_block_date as ts, call_tx_hash as hash, dart, from_utf8(bytearray_rtrim(i)) as ilk
    --     FROM maker_ethereum.vat_call_grab
    --     where call_block_date >= timestamp'2025-09-01 00:00'
    --     and from_utf8(bytearray_rtrim(i)) in ('ALLOCATOR-SPARK-A', 'ALLOCATOR-OBEX-A' /*,'ALLOCATOR-BLOOM-A'*/)
    -- )
    -- , star_revenues_preunioned as (
    --     SELECT ts, hash, ilk, (dart * rate / power(10, 45)) as value
    --     FROM cum_rates as cr join star_revenues_debt as srd using(ilk, dart, hash, ts) 
    -- )
    -- , star_revenues as (
    --     -- Increase revenues
    --     -- Already accounted as assets in Loan Actions
    --     SELECT ts, hash, ilk
    --         , CASE 
    --             WHEN ILK = 'ALLOCATOR-SPARK-A' THEN 34110 -- Spark SubDAO Revenues
    --             WHEN ilk = 'ALLOCATOR-BLOOM-A' THEN 34111 -- Grove SubDAO Revenues
    --             WHEN ilk = 'ALLOCATOR-OBEX-A' THEN 34112 -- Obex SubDAO Revenues
    --         END as account_id 
    --         , value
    --     FROM star_revenues_preunioned
    --     UNION ALL
    --     -- Decrease liabilities
    --     SELECT ts, hash, ilk
    --         , 21120 as account_id -- dai (non-interest bearing)
    --         , -value as value
    --     FROM star_revenues_preunioned
    -- )
    
    -- ***************************************************************************
    -- **************************** D S R   F L O W S ****************************
    -- ***************************************************************************
    , dsr_flows_preunioned as (
        select
            call_block_time as ts,
            call_tx_hash as hash,
            case
                when src = 0x197e90f9fad81970ba7976f33cbd77088e5d7cf7 -- pot
                then -1
                else 1
            end * 1e-45 * rad as dsr_flow
        from maker_ethereum.vat_call_move m
        where call_success
        and 0x197e90f9fad81970ba7976f33cbd77088e5d7cf7 in (src, dst) -- pot
    ),
    dsr_flows as (
        select
            ts,
            hash,
            21110 as account_id, -- dai (interest-bearing)
            dsr_flow as value    -- increased liability
        from dsr_flows_preunioned
        union all
        select
            ts,
            hash,
            21120 as account_id, -- dai (non-interest bearing)
            -dsr_flow as value   -- decreased liability
        from dsr_flows_preunioned
    ),
    dsr_expenses as (
        select
            call_block_time as ts,
            call_tx_hash as hash,
            31610 as account_id,       -- circulating dai (direct expenses)
            -SUM(rad/POWER(10, 45)) as value -- decreased equity
        from maker_ethereum.vat_call_suck
        where u = 0xa950524441892a31ebddf91d3ceefa04bf454466 -- Vow
        and v = 0x197e90f9fad81970ba7976f33cbd77088e5d7cf7   -- Pot
        and call_success
        group by 1, 2
        union all
        select
            call_block_time as ts,
            call_tx_hash as hash,
            21110 as account_id,      -- dai (interest bearing)
            SUM(rad/POWER(10, 45)) as value -- increased liability
        from maker_ethereum.vat_call_suck
        where u = 0xa950524441892a31ebddf91d3ceefa04bf454466 -- Vow
        and v = 0x197e90f9fad81970ba7976f33cbd77088e5d7cf7   -- Pot
        and call_success
        group by 1, 2
    ),
    -- ***************************************************************************
    -- ************************** P A U S E   P R O X Y **************************
    -- ***************************************************************************
    -- Find what is pause proxy, how it works, etc
    pause_proxy_mkr_trxns_raw as (
        select
            evt_block_time as ts,
            evt_tx_hash as hash,
            case
                when "from" = 0xbe8e3e3618f7474f8cb1d074a26affef007e98fb then value
                when "to" = 0xbe8e3e3618f7474f8cb1d074a26affef007e98fb then -value
            end as expense,
            case
                when "from" = 0xbe8e3e3618f7474f8cb1d074a26affef007e98fb then "to"
                when "to" = 0xbe8e3e3618f7474f8cb1d074a26affef007e98fb then "from"
            end as address
        from maker_ethereum.mkr_evt_transfer
        where 0xbe8e3e3618f7474f8cb1d074a26affef007e98fb in ("from", "to") -- treasury
          and "from" != 0x8ee7d9235e01e6b42345120b5d270bdb763624c7 -- MakerDAO Multisig
          and "from" != 0x517f9dd285e75b599234f7221227339478d0fcc8 -- UNI V2 MKR/DAI LP
          and "from" != "to"
    ),
    mkr_vest_trxns as (
        select
            evt_tx_hash as hash,
            1 as vested
        from maker_ethereum.dssvesttransferrable_evt_vest
    ),
    pause_proxy_mkr_trxns_preunion as (
        select
            ts,
            hash,
            case
                when vested is not null
                then 32120 -- Vested MKR Token Expenses
                else 32110 -- Direct MKR Token Expenses
            end as account_id,
            -expense / 1e18 as value
        from pause_proxy_mkr_trxns_raw
        left join mkr_vest_trxns using (hash)
    ),
    pause_proxy_mkr_trxns as (
        select
            ts,
            hash,
            account_id,
            value
        from pause_proxy_mkr_trxns_preunion
        union all
        select
            ts,
            hash,
            32210 as account_id, -- MKR Contra Equity
            -value
        from pause_proxy_mkr_trxns_preunion
    ),
    -- ***************************************************************************
    -- ********************* A C C O U N T I N G   P L U G S *********************
    -- ***************************************************************************
    hashless_trxns as (
        select
            timestamp '2022-11-01 00:00' as ts,
            null as hash, -- 'noHash:movingGusdPSMBalancefromNonYieldingToYielding'
            13410 as account_id, -- Non-Yielding Stablecoin (PSM)
            -222632234.27 as value,
            'DAI' as token,
            'PSM-GUSD-A' as ilk
        union all
        select
            timestamp '2022-11-01 00:00' as ts,
            null as hash, -- 'noHash:movingGusdPSMBalancefromNonYieldingToYielding'
            13411 as account_id, -- Yielding Stablecoin (PSM)
            222632234.27 as value,
            'DAI' as token,
            'PSM-GUSD-A' as ilk
    ),
    -- ***************************************************************************
    -- ***************************** T R E A S U R Y *****************************
    -- ***************************************************************************
    treasury_flows_preunioned as (
        select
            evt.evt_block_time as ts,
            evt.evt_tx_hash as hash,
            t.token,
            sum(
                case
                    when evt."to" = 0xbe8e3e3618f7474f8cb1d074a26affef007e98fb then evt.value / pow(10, t.decimals)
                    when evt."from" = 0xbe8e3e3618f7474f8cb1d074a26affef007e98fb then -evt.value / pow(10, t.decimals)
                    else 0
                end
            ) as value
        from erc20_ethereum.evt_transfer evt
        join query_3150055 t -- maker - treasury erc20s
            on evt.contract_address = t.contract_address
        where evt.evt_block_time > date '2019-11-01'
        and 0xbe8e3e3618f7474f8cb1d074a26affef007e98fb in (evt."to", evt."from")
        group by 1, 2, 3
    ),
    treasury_flows as (
        select
            ts,
            hash,
            33110 as account_id, -- DS Pause Proxy
            value as value,      -- increased equity
            token
        from treasury_flows_preunioned
        union all
        select
            ts,
            hash,
            14620 as account_id, -- DS Pause Proxy
            value as value,      -- increased assets
            token
        from treasury_flows_preunioned
    ),
    -- ***************************************************************************
    -- ************************** S I N   I N F L O W S **************************
    -- ***************************************************************************
    -- ('v' is source, 'u' is destination)
    sin_inflows as (
        select
            call_block_time as ts,
            call_tx_hash hash,
            31510 as account_id,      -- Sin Inflow
            sum(rad/POWER(10, 45)) as value -- increased equity
        from maker_ethereum.vat_call_suck
        where v = 0xa950524441892a31ebddf91d3ceefa04bf454466 -- Vow
        and call_success
        group by 1, 2
        union all
        select
            call_block_time ts,
            call_tx_hash hash,
            21120 as account_id,       -- dai (non-interest bearing)
            -sum(rad/POWER(10, 45)) as value -- decreased liability
        from maker_ethereum.vat_call_suck
        where v = 0xa950524441892a31ebddf91d3ceefa04bf454466 -- Vow
        and call_success
        group by 1, 2
    ),
    -- ***************************************************************************
    -- ******************** O T H E R   S I N   I N F L O W S ********************
    -- ***************************************************************************
    -- ('v' is source, 'u' is destination)
    other_sin_outflows as (
        select
            call_block_time as ts,
            call_tx_hash as hash,
            31520 as account_id,       -- Sin Outflow
            -sum(rad/POWER(10, 45)) as value -- reduced equity
        from maker_ethereum.vat_call_suck
        where u = 0xa950524441892a31ebddf91d3ceefa04bf454466 -- Vow
        and v not in (
            0x197e90f9fad81970ba7976f33cbd77088e5d7cf7, -- Pot/Savings DAI
            0xa3931d71877c0e7a3148cb7eb4463524fec27fbd, -- Savings USDs
            0xbe8e3e3618f7474f8cb1d074a26affef007e98fb, -- Pause Proxy
            0x2cc583c0aacdac9e23cb601fda8f1a0c56cdcb71, -- Vest Dai Legacy
            0xa4c22f0e25c6630b2017979acf1f865e94695c4b, -- Vest Dai
            0x99cd4ec3f88a45940936f469e4bb72a2a701eeb9, -- stUSDS
            0xd889477102e8c4a857b78fcc2f134535176ec1fc) -- Kicker (profit distribution gatekeeper making productive use of USDS to Burn Engine or Sky Token Rewards subDAOs)
        and call_success
        group by 1, 2
        union all
        select
            call_block_time ts,
            call_tx_hash hash,
            21120 as account_id,      -- dai (non-interest bearing)
            sum(rad/POWER(10, 45)) as value -- increased liability
        from maker_ethereum.vat_call_suck
        where u = 0xa950524441892a31ebddf91d3ceefa04bf454466 -- Vow
        and v not in (
            0x197e90f9fad81970ba7976f33cbd77088e5d7cf7, -- Pot/Savings DAI
            0xa3931d71877c0e7a3148cb7eb4463524fec27fbd, -- Savings USDs
            0xbe8e3e3618f7474f8cb1d074a26affef007e98fb, -- Pause Proxy
            0x2cc583c0aacdac9e23cb601fda8f1a0c56cdcb71, -- Vest Dai Legacy
            0xa4c22f0e25c6630b2017979acf1f865e94695c4b, -- Vest Dai
            0x99cd4ec3f88a45940936f469e4bb72a2a701eeb9, -- stUSDS
            0xd889477102e8c4a857b78fcc2f134535176ec1fc) -- Kicker (profit distribution gatekeeper making productive use of USDS to Burn Engine or Sky Token Rewards subDAOs)
        and call_success
        group by 1, 2
    ),
    -- ***************************************************************************
    -- *************** M K R   V E S T   C R E A T E S / Y A N K S ***************
    -- ***************************************************************************
    create_mkr_vests_raw as (
        select
            call_block_time as ts,
            call_tx_hash as hash,
            output_id,
            _bgn,
            _tau,
            1e-18 * _tot as total_mkr
        from maker_ethereum.dssvesttransferrable_call_create
        where call_success
    ),
    yanks_raw as (
        select
            call_block_time as ts,
            call_tx_hash as hash,
            from_unixtime(_end) as end_ts,
            _id
        from maker_ethereum.dssvesttransferrable_call_yank
        where call_success
    ),
    yanks as (
        select
            y.ts,
            y.hash,
            y._id,
            from_unixtime(c._bgn) as begin_time,
            case
                when y.end_ts > y.ts
                then y.end_ts
                else y.ts
            end as end_time,
            c._tau as _tau,
            c.total_mkr as original_total_mkr,
            (1 - (to_unixtime(case when y.end_ts > y.ts then y.end_ts else y.ts end) - c._bgn * 1e0) / c._tau) * c.total_mkr as yanked_mkr
        from yanks_raw y
        left join create_mkr_vests_raw c
            on y._id = c.output_id
    ),
    mkr_vest_creates_yanks as (
        select
            ts,
            hash,
            32110 as account_id, -- Direct MKR Token Expenses
            -total_mkr as value
        from create_mkr_vests_raw
        union all
        select
            ts,
            hash,
            32120 as account_id, -- Vested MKR Token Expenses
            total_mkr as value
        from create_mkr_vests_raw
        union all
        select
            ts,
            hash,
            32110 as account_id, -- Direct MKR Token Expenses (MKR expense reversed (yanked))
            yanked_mkr as value
        from yanks
        union all
        select
            ts,
            hash,
            32120 as account_id, -- Vested MKR Token Expenses (MKR in vest contracts yanked (decreases))
            -yanked_mkr as value
        from yanks
    ),
    final as (
        select
            ts,
            hash,
            account_id,
            value,
            'DAI' as token,
            'Interest Accruals' as descriptor,
            ilk
        from main_interest_accruals
        union all
        select
            ts,
            hash,
            account_id,
            value,
            'DAI' as token,
            'Loan Draws/Repays' as descriptor,
            ilk
        from loan_actions
        union all
        select
            ts,
            hash,
            account_id,
            value,
            'DAI' as token,
            'DSR Flows' as descriptor,
            null as ilk
        from dsr_flows
        union all
        select
            ts,
            hash,
            account_id,
            value,
            'DAI' as token,
            'DSR Expenses' as descriptor,
            null as ilk
        from dsr_expenses
        union all
        select
            ts,
            hash,
            account_id,
            value,
            'MKR' as token,
            'MKR Pause Proxy Trxns' as descriptor,
            null as ilk
        from pause_proxy_mkr_trxns
        union all
        select
            ts,
            hash,
            account_id,
            value,
            token,
            'Accounting Plugs' as descriptor,
            ilk
        from hashless_trxns
        union all
        select
            ts,
            hash,
            account_id,
            value,
            token,
            'Treasury Flows' as descriptor,
            null as ilk
        from treasury_flows
        union all
        select
            ts,
            hash,
            account_id,
            value,
            'DAI' as token,
            'Sin Inflows' as descriptor,
            null as ilk
        from sin_inflows
        union all
        select
            ts,
            hash,
            account_id,
            value,
            'DAI' as token,
            'Other Sin Outflows' as descriptor,
            null as ilk
        from other_sin_outflows
        union all
        select
            ts,
            hash,
            account_id,
            value,
            'MKR' as token,
            'MKR Vest Creates/Yanks' as descriptor,
            null as ilk
        from mkr_vest_creates_yanks
        union all
        select
            ts,
            hash,
            account_id,
            value,
            'DAI' as token,
            descriptor,
            ilk
        from idle_txs
        union all
        select ts,
            hash,
            account_id,
            value,
            'DAI' as token,
            'RWA003-A Plugs' as descriptor,
            ilk
        FROM loan_actions_plugged
        -- UNION ALL
        -- SELECT ts,
        --     hash,
        --     account_id,
        --     value,
        --     'DAI' as token,
        --     'Star Revenues' as descriptor,
        --     ilk
        -- FROM star_revenues
    )

-- test query
-- select sum(value) as value from final where date(ts) < date '2024-05-21' -- 10,715,291,955.4

select * from final