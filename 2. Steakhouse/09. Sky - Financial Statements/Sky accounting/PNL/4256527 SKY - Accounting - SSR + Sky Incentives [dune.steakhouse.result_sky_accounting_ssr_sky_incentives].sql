/*
-- @source: https://dune.com/queries/4203066
-- @title: SKY SSR Incentives
-- @author: Steakhouse Financial
-- @version:
    - 1.0 - 2024-10-26 - Initial version
    - 2.0 - 2024-10-31 - Add preliminary logic for SKY incentives distributions 
    - 3.0 - 2024-11-01 - Add SKY incentives tracking with accounting codes
    - 4.0 - 2024-11-02 - Add comments defining query
    - 5.0 - 2024-11-04 - Modify direct expenses for SKY as negative.
                        -> Make changes reflecting the DAI migrated to USDS 
    - 6.0 - 2025-05-05 - Deduct SKY reward expenses
                        -> Remove SSR Expenses (impacting DSR calc). Incorrect def.
    - 7.0 - 2025-05-07 - Add SKY Burns through movements of the Splitter contract.
    - 8.0 - 2025-05-13 - Exclude Pre-minted USDS
    - 9.0 - 2025-05-15 - Assign Sky rewards as Reserve SKY Surplus
                        -> Add SSR Expenses
    - 10.0 - 2025-05-23 - Change acount id for Sky incentives to 32111
                        -> Add SSR Expenses
                        -> Decompose USDS TXs
    - 11.0 - 2025-05-24 - Exclude the sUSDS contract in usds_txs (its not a conversion from DAI<->USDS)
                        -> Add token field in usds_flows to distinguish DAI and USDS
                        -> Modify the SSR Flows CTE and SSR Expenses
    - 12.0 - 2025-05-25 - Merge new changes into production
    - 13.0 - 2025-07-23 - Split SKY Staking revenues and SKY Burns
    - 14.0 - 2025-10-10 - Add Spark Surplus Buffer into overall buffer
    - 15.0 - 2025-10-20 - Add Revenues split from Spark and Grove
                        -> Add Grove Surplus Buffer
    - 16.0 - 2025-12-11 - Change account_id codes for surplus buffer to 31810, 31820, 31830, 31840
    - 17.0 - 2025-12-12 - Modify account_id codes for surplus buffer in the asset side 
    - 18.0 - 2026-01-06 - Add stUSD contract in the ssr_flows and ssr_expenses
*/

-- ***************************************************************************
-- **************************** S S R   F L O W S ****************************
-- ***************************************************************************
WITH 
usds_txs as (
    select
        call_block_time as ts,
        call_tx_hash as hash,
        CASE WHEN src = 0x3c0f895007ca717aa01c8693e59df1e8c3777feb THEN dst ELSE src END as address,
        (case
            when src = 0x3c0f895007ca717aa01c8693e59df1e8c3777feb -- USDS Join
            then -1
            else 1
        end / POWER(10, 45)) * rad as value
    from maker_ethereum.vat_call_move m
    where call_success
    and 0x3c0f895007ca717aa01c8693e59df1e8c3777feb in (src, dst) -- USDS Join
    AND rad / POWER(10, 45) != 0
    UNION ALL
    -- Missing TX in above table
    SELECT evt_block_time as ts
        , evt_tx_hash as hash
        , 0x3c0f895007ca717aa01c8693e59df1e8c3777feb as placeholder_address
        , CASE 
            WHEN "from" = 0x0000000000000000000000000000000000000000 THEN 1 -- Mint Address
            WHEN "from" = 0x3225737a9bbb6473cb4a45b7244aca2befdb276a THEN -1 -- DAIUSDS CA
        END * (value/POWER(10, 18)) as value
    FROM sky_ethereum.usds_evt_transfer
    WHERE evt_tx_hash = 0xebd26968dd86ac4f3bbb8fa4c5280bd44122e59ef9bfa9e57f3fdcc568a58510
    and "from" NOT IN (0x1f2f10d1c40777ae1da742455c65828ff36df387 -- JaredFromSubway
    , 0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD)  -- DAIUSDS CA (Handled in SSR Flows)
)
, usds_txs_formatted as (
    SELECT ts, hash
        , CASE 
            WHEN address = 0xf86141a5657cf52aeb3e30ebcca5ad3a8f714b89 AND value > 0 THEN 'Migrate DAI->USDS'
            WHEN address = 0xf86141a5657cf52aeb3e30ebcca5ad3a8f714b89 AND value < 0 THEN 'Migrate USDS->DAI'
            WHEN address = 0xa188eec8f81263234da3622a406892f3d630f98c AND value > 0 THEN 'USDS PSM USDC->USDS'
            WHEN address = 0xa188eec8f81263234da3622a406892f3d630f98c AND value < 0 THEN 'USDS PSM USDS->USDC'
            WHEN address = 0x3225737a9bbb6473cb4a45b7244aca2befdb276a AND value > 0 THEN 'USDSJoin DAI->USDS'
            WHEN address = 0x3225737a9bbb6473cb4a45b7244aca2befdb276a AND value < 0 THEN 'USDSJoin USDS->DAI'
            WHEN address = 0xce01c90de7fd1bcfa39e237fe6d8d9f569e8a6a3 AND value > 0 THEN 'Borrowed USDS (LockstageEngine)'
            WHEN address = 0xce01c90de7fd1bcfa39e237fe6d8d9f569e8a6a3 AND value < 0 THEN 'Repaid USDS (LockstageEngine)'
            ELSE 'USDS Flows'
        END as descriptor
        , value
    FROM usds_txs
    WHERE address NOT IN (
        0xbf7111f13386d23cb2fba5a538107a73f6872bcf -- Splitter: Active (handled in sky_burns_preunioned)
        , 0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD -- Savings USDS (handled in ssr_flows_preunioned)
        , 0x99cd4ec3f88a45940936f469e4bb72a2a701eeb9 -- Staked USDS (handled in ssr_flows_preunioned) 
    )
)
, ssr_flows_preunioned as (
    -- sUSDS
    SELECT evt_block_time as ts, assets/POWER(10, 18) as ssr_flow, evt_tx_hash as hash
    FROM sky_ethereum.susds_evt_deposit where assets != 0
    UNION ALL
    SELECT evt_block_time as ts, -assets/POWER(10, 18) as ssr_flow, evt_tx_hash as hash
    FROM sky_ethereum.susds_evt_withdraw where assets != 0
    UNION ALL
    -- stUSDS
    SELECT evt_block_time as ts, assets/POWER(10, 18) as ssr_flow, evt_tx_hash as hash
    FROM sky_ethereum.stusds_evt_deposit where assets != 0
    UNION ALL
    SELECT evt_block_time as ts, -assets/POWER(10, 18) as ssr_flow, evt_tx_hash as hash
    FROM sky_ethereum.stusds_evt_withdraw where assets != 0
),
ssr_flows as (
    select
        ts,
        hash,
        21111 as account_id, -- USDS (interest-bearing)
        ssr_flow as value    -- increased liability
    from ssr_flows_preunioned
    union all
    select
        ts,
        hash,
        21121 as account_id, -- USDS (non-interest bearing)
        -ssr_flow as value   -- decreased liability
    from ssr_flows_preunioned
),
usds_flows as (
  SELECT ts
    , hash
    , 'USDS' as token
    , descriptor
    , 21121 as account_id -- USDS (non-interest bearing)
    , value
  FROM usds_txs_formatted
  
  UNION ALL
  
  SELECT ts
    , hash
    , 'DAI' as token
    , descriptor
    , 21120 as account_id -- DAI (non-interest bearing)
    , -value
  FROM usds_txs_formatted
)
, ssr_expenses_preunioned as (
    -- The SKY Savings Rate issues interest USDS to be stored into the Pot
    -- This becomes an issued liability for the protocol
    SELECT evt_block_time as ts
        , evt_tx_hash as hash
        , SUM(diff/POWER(10, 18)) as value
    FROM sky_ethereum.susds_evt_drip
    WHERE diff != 0
    GROUP BY 1, 2
    union all
    -- stUSDS
    SELECT evt_block_time as ts
        , evt_tx_hash as hash
        , SUM(diff/POWER(10, 18)) as value
    FROM sky_ethereum.stusds_evt_drip
    WHERE diff != 0
    GROUP BY 1, 2
)
, ssr_expenses as (
    select ts, hash
        , 21111 as account_id -- USDS (interest bearing)
        , value -- increased liability
    from ssr_expenses_preunioned
    UNION ALL
    select ts, hash
        , 31611 as account_id -- Circulating USDS (direct expenses)
        , -value -- Decreased equity
    from ssr_expenses_preunioned
)
, spark_subdao_txns as (
    -- Revenues accrued from Spark positions
    -- https://info.sky.money/capital-buffer
    SELECT evt_block_time as ts
        , evt_tx_hash as hash
        , CASE 
            WHEN "from" = 0x3300f198988e4C9C63F75dF86De36421f06af8c4 THEN -1
            ELSE 1
        END * (value/POWER(10, 18)) as value
    FROM sky_ethereum.usds_evt_transfer
    WHERE 0x3300f198988e4C9C63F75dF86De36421f06af8c4 in ("from", "to")
)
, grove_subdao_txns as (
    -- Revenues accrued from RWA positions
    -- https://info.sky.money/capital-buffer
    SELECT evt_block_time as ts
        , evt_tx_hash as hash
        , CASE 
            WHEN "from" = 0x1369f7b2b38c76B6478c0f0E66D94923421891Ba THEN -1
            ELSE 1
        END * (value/POWER(10, 18)) as value
    FROM sky_ethereum.usds_evt_transfer
    WHERE 0x1369f7b2b38c76B6478c0f0E66D94923421891Ba in ("from", "to")
)
, obex_subdao_txns as (
    -- Positions in Maple finance
    -- https://info.sky.money/capital-buffer
    SELECT evt_block_time as ts
        , evt_tx_hash as hash
        , CASE 
            WHEN "from" = 0x8be042581f581E3620e29F213EA8b94afA1C8071 THEN -1
            ELSE 1
        END * (value/POWER(10, 18)) as value
    FROM sky_ethereum.usds_evt_transfer
    WHERE 0x8be042581f581E3620e29F213EA8b94afA1C8071 in ("from", "to")
)
, subdao_surplus_txns as (
    select ts
        , hash
        , 'Spark SubDAO Treasury' as descriptor
        , 31810 as account_id -- Spark subDAO Allocation
        , value
    FROM spark_subdao_txns
    UNION ALL
    select ts
        , hash
        , 'Spark SubDAO Treasury' as descriptor
        , 16010 as account_id -- Spark SubDAO Backstop Capital
        , value
    FROM spark_subdao_txns
    UNION ALL

    select ts
        , hash
        , 'Grove SubDAO Treasury' as descriptor
        , 31820 as account_id -- Grove subDAO Allocation
        , value
    FROM grove_subdao_txns
    UNION ALL
    select ts
        , hash
        , 'Grove SubDAO Treasury' as descriptor
        , 16020 as account_id -- Grove SubDAO Backstop Capital
        , value
    FROM grove_subdao_txns
    UNION ALL

    select ts
        , hash
        , 'Obex SubDAO Treasury' as descriptor
        , 31830 as account_id -- Obex subDAO Allocation
        , value
    FROM obex_subdao_txns
    UNION ALL
    select ts
        , hash
        , 'Obex SubDAO Treasury' as descriptor
        , 16030 as account_id -- Obex SubDAO Backstop Capital
        , value
    FROM obex_subdao_txns
)
, sky_incentive_txns as (
    SELECT evt_block_time as ts
        , evt_tx_hash as hash
        , (value / POWER(10, 18)) as value
    FROM sky_ethereum.SKY_evt_Transfer
        -- Sky - Incentives Facilitators Wallets
        JOIN query_4231088 as facilitator_wallets
            ON "from" = facilitator_wallets.wallet_address
        -- Sky - Incentive  Contracts
        JOIN query_4231073 as contracts
            ON "to" = contracts.contract_address
)
, sky_incentives as (
    -- Similar structure to directed MKR expenses: 
    -- LN 232-258 https://dune.com/queries/3150125 -- Maker Interest Accruals + DSR [old]
    SELECT ts
        , hash
        , 32111 as account_id -- Direct SKY Rewards
        , -value as value
    FROM sky_incentive_txns

    UNION ALL
    
    SELECT ts
        , hash
        , 32211 as account_id -- SKY Contra Equity
        , value as value
    FROM sky_incentive_txns
),
-- ***************************************************************************
-- ************************* S K Y   B U Y B A C K S *************************
-- ***************************************************************************
-- Stablecoin movements with the Splitter which is used to buyback SKY and store
-- in the SKY Pause Proxy.
sky_burns_preunioned as (
    select
        call_block_time as ts,
        call_tx_hash as hash,
        sum(rad / POWER(10, 45)) as value
    from maker_ethereum.vat_call_move
    where src = 0xa950524441892a31ebddf91d3ceefa04bf454466 -- Vow
    and dst in (
        0xbf7111f13386d23cb2fba5a538107a73f6872bcf -- Splitter: Active
    )
    and call_success
    group by 1, 2
),
sky_staking_preunioned as (
    SELECT evt_block_time as ts
        , evt_tx_hash as hash
        , wad / POWER(10, 18) as value
    FROM sky_ethereum.usdsjoin_evt_exit
    WHERE evt_block_number >= 22617817 -- 2025-06-02 15:15
    and "caller" = 0xbf7111f13386d23cb2fba5a538107a73f6872bcf -- Spliter: Active
    and usr = 0x38E4254bD82ED5Ee97CD1C4278FAae748d998865 -- StakingRewards USDS Pool
),
sky_burns as (
    select
        ts,
        hash,
        31421 as account_id,  -- SKY Burns
        -value as value       -- decreased equity
    from sky_burns_preunioned
    union all
    select
        ts,
        hash,
        21121 as account_id, -- USDS
        value as value       -- increased liability
    from sky_burns_preunioned
),
sky_staking as (
    -- Splitting the Sky Burns component to Burns and SKY Stakers
    SELECT ts,
        hash,
        31421 as account_id,  -- SKY Burns
        value as value        -- increased equity
    FROM sky_staking_preunioned
    UNION ALL
    SELECT ts,
        hash,
        31640 as account_id, -- SKY Staking Expenses
        -value as value      -- decreased equity
    FROM sky_staking_preunioned
)
-- ***************************************************************************
-- ********************** I D L E   U S D S    F L O W ***********************
-- ***************************************************************************

, preminted_usds_flows as (
    SELECT evt_block_time as ts
        , CASE WHEN "to" = 0x92afd6f2385a90e44da3a8b60fe36f6cbe1d8709 THEN 1 ELSE -1 END * (value / POWER(10, 18)) AS value
        , evt_tx_hash as hash
    FROM sky_arbitrum.usds_evt_transfer
    WHERE evt_block_time >= TIMESTAMP'2025-02-03 23:23:48'
    and 0x92afd6f2385a90e44da3a8b60fe36f6cbe1d8709 in ("from", "to") -- Arbitrum: ALM_Proxy
    UNION ALL
    SELECT evt_block_time as ts
        , CASE WHEN "to" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E THEN 1 ELSE -1 END * (value / POWER(10, 18)) AS value
        , evt_tx_hash as hash
    FROM sky_base.usds_evt_transfer
    WHERE evt_block_time >= TIMESTAMP'2024-10-23 14:43:11'
    and 0x2917956eFF0B5eaF030abDB4EF4296DF775009cA in ("from", "to") -- Base: ALM_Proxy
    UNION ALL
    SELECT evt_block_time as ts
        , CASE WHEN "to" = 0x345E368fcCd62266B3f5F37C9a131FD1c39f5869 THEN 1 ELSE -1 END * (value / POWER(10, 18)) AS value
        , evt_tx_hash as hash
    FROM erc20_unichain.evt_transfer
    WHERE evt_block_time >= TIMESTAMP'2025-05-15 02:33:33'
    and contract_address = 0x7E10036Acc4B56d4dFCa3b77810356CE52313F9C
    and 0x345E368fcCd62266B3f5F37C9a131FD1c39f5869 in ("from", "to") -- Unichain: ALM_Proxy
    UNION ALL
    SELECT evt_block_time as ts
        , CASE WHEN "to" = 0x876664f0c9Ff24D1aa355Ce9f1680AE1A5bf36fB THEN 1 ELSE -1 END * (value / POWER(10, 18)) AS value
        , evt_tx_hash as hash
    FROM erc20_optimism.evt_transfer
    WHERE evt_block_time >= TIMESTAMP'2025-05-07 12:29:55'
    and contract_address = 0x4f13a96ec5c4cf34e442b46bbd98a0791f20edc3
    and 0x876664f0c9Ff24D1aa355Ce9f1680AE1A5bf36fB in ("from", "to") -- Optimism: ALM_Proxy
    UNION ALL
    SELECT evt_block_time as ts
        , CASE WHEN "to" = 0x2b05f8e1cacc6974fd79a673a341fe1f58d27266 THEN 1 ELSE -1 END * (value / POWER(10, 18)) AS value
        , evt_tx_hash as hash
    FROM sky_arbitrum.usds_evt_transfer
    WHERE evt_block_time >= TIMESTAMP'2025-02-03 23:23:48'
    and 0x2b05f8e1cacc6974fd79a673a341fe1f58d27266 in ("from", "to") -- Arbitrum: PSM3
    UNION ALL
    SELECT evt_block_time as ts
        , CASE WHEN "to" = 0x1601843c5e9bc251a3272907010afa41fa18347e THEN 1 ELSE -1 END * (value / POWER(10, 18)) AS value
        , evt_tx_hash as hash
    FROM sky_base.usds_evt_transfer
    WHERE evt_block_time >= TIMESTAMP'2024-10-23 14:43:11'
    and 0x1601843c5e9bc251a3272907010afa41fa18347e in ("from", "to") -- Base: PSM3
    UNION ALL
    SELECT evt_block_time as ts
        , CASE WHEN "to" = 0x7b42Ed932f26509465F7cE3FAF76FfCe1275312f THEN 1 ELSE -1 END * (value / POWER(10, 18)) AS value
        , evt_tx_hash as hash
    FROM erc20_unichain.evt_transfer
    WHERE evt_block_time >= TIMESTAMP'2025-05-13 06:07:50'
    and contract_address = 0x7E10036Acc4B56d4dFCa3b77810356CE52313F9C
    and 0x7b42Ed932f26509465F7cE3FAF76FfCe1275312f in ("from", "to") -- Unichain: PSM3
    UNION ALL
    SELECT evt_block_time as ts
        , CASE WHEN "to" = 0xe0F9978b907853F354d79188A3dEfbD41978af62 THEN 1 ELSE -1 END * (value / POWER(10, 18)) AS value
        , evt_tx_hash as hash
    FROM erc20_optimism.evt_transfer
    WHERE evt_block_time >= TIMESTAMP'2025-05-07 11:50:31'
    and contract_address = 0x4f13a96ec5c4cf34e442b46bbd98a0791f20edc3
    and 0xe0F9978b907853F354d79188A3dEfbD41978af62 in ("from", "to") -- Optimism: PSM3
)
, preminted_susds_flows as (
        SELECT evt_block_time as ts
        , CASE WHEN "to" = 0x2b05f8e1cacc6974fd79a673a341fe1f58d27266 THEN 1 ELSE -1 END * (value / POWER(10, 18)) AS value
        , evt_tx_hash as hash
    FROM sky_arbitrum.susds_evt_transfer
    WHERE evt_block_time >= TIMESTAMP'2025-02-03 23:23:48'
    and 0x2b05f8e1cacc6974fd79a673a341fe1f58d27266 in ("from", "to") -- Arbitrum: PSM3
    UNION ALL
    SELECT evt_block_time as ts
        , CASE WHEN "to" = 0x1601843c5e9bc251a3272907010afa41fa18347e THEN 1 ELSE -1 END * (value / POWER(10, 18)) AS value
        , evt_tx_hash as hash
    FROM sky_base.susds_evt_transfer
    WHERE evt_block_time >= TIMESTAMP'2024-10-23 14:43:11'
    and 0x1601843c5e9bc251a3272907010afa41fa18347e in ("from", "to") -- Base: PSM3
    UNION ALL
    SELECT evt_block_time as ts
        , CASE WHEN "to" = 0x7b42ed932f26509465f7ce3faf76ffce1275312f THEN 1 ELSE -1 END * (value / POWER(10, 18)) AS value
        , evt_tx_hash as hash
    FROM erc20_unichain.evt_transfer
    WHERE evt_block_time >= TIMESTAMP'2025-05-13 06:07:50'
    and contract_address = 0xA06b10Db9F390990364A3984C04FaDf1c13691b5 -- SUSDS
    and 0x7b42ed932f26509465f7ce3faf76ffce1275312f in ("from", "to") -- Unichain: PSM3
    UNION ALL
    SELECT evt_block_time as ts
        , CASE WHEN "to" = 0xe0F9978b907853F354d79188A3dEfbD41978af62 THEN 1 ELSE -1 END * (value / POWER(10, 18)) AS value
        , evt_tx_hash as hash
    FROM erc20_optimism.evt_transfer
    WHERE evt_block_time >= TIMESTAMP'2025-05-07 11:50:31'
    and contract_address = 0xb5b2dc7fd34c249f4be7fb1fcea07950784229e0 -- SUSDS
    and 0xe0F9978b907853F354d79188A3dEfbD41978af62 in ("from", "to") -- Optimism: PSM3
    
    -- union all
    -- SELECT evt_block_time as ts
    --     , CASE WHEN "to" = 0x92afd6f2385a90e44da3a8b60fe36f6cbe1d8709 THEN 1 ELSE -1 END * (value / POWER(10, 18)) AS value
    --     , evt_tx_hash as hash
    -- FROM sky_arbitrum.susds_evt_transfer
    -- WHERE evt_block_time >= TIMESTAMP'2025-02-03 23:23:48'
    -- and 0x92afd6f2385a90e44da3a8b60fe36f6cbe1d8709 in ("from", "to") -- Arbitrum: ALM_Proxy
    -- UNION ALL
    -- SELECT evt_block_time as ts
    --     , CASE WHEN "to" = 0x2917956eFF0B5eaF030abDB4EF4296DF775009cA THEN 1 ELSE -1 END * (value / POWER(10, 18)) AS value
    --     , evt_tx_hash as hash
    -- FROM sky_base.susds_evt_transfer
    -- WHERE evt_block_time >= TIMESTAMP'2024-10-23 14:43:11'
    -- and 0x2917956eFF0B5eaF030abDB4EF4296DF775009cA in ("from", "to") -- Base: ALM_Proxy
    -- UNION ALL
    -- SELECT evt_block_time as ts
    --     , CASE WHEN "to" = 0x345E368fcCd62266B3f5F37C9a131FD1c39f5869 THEN 1 ELSE -1 END * (value / POWER(10, 18)) AS value
    --     , evt_tx_hash as hash
    -- FROM erc20_unichain.evt_transfer
    -- WHERE evt_block_time >= TIMESTAMP'2025-05-13 06:07:50'
    -- and contract_address = 0xA06b10Db9F390990364A3984C04FaDf1c13691b5 -- SUSDS
    -- and 0x345E368fcCd62266B3f5F37C9a131FD1c39f5869 in ("from", "to") -- Unichain: ALM_Proxy
    -- UNION ALL
    -- SELECT evt_block_time as ts
    --     , CASE WHEN "to" = 0x876664f0c9Ff24D1aa355Ce9f1680AE1A5bf36fB THEN 1 ELSE -1 END * (value / POWER(10, 18)) AS value
    --     , evt_tx_hash as hash
    -- FROM erc20_optimism.evt_transfer
    -- WHERE evt_block_time >= TIMESTAMP'2025-05-07 11:50:31'
    -- and contract_address = 0xb5b2dc7fd34c249f4be7fb1fcea07950784229e0 -- SUSDS
    -- and 0x876664f0c9Ff24D1aa355Ce9f1680AE1A5bf36fB in ("from", "to") -- Optimism: ALM_Proxy
    UNION ALL
    SELECT evt_block_time as ts
        , CASE WHEN "to" = 0x00836fe54625be242bcfa286207795405ca4fd10 THEN 1 ELSE -1 END * (value / POWER(10, 18)) AS value
        , evt_tx_hash as hash
    FROM sky_ethereum.susds_evt_transfer
    WHERE evt_block_time >= TIMESTAMP'2025-04-07 07:25:23'
    and 0x00836fe54625be242bcfa286207795405ca4fd10 in ("from", "to") -- Curve: sUSDSUSDT
)
, susds_oracle_price as (
    SELECT dt, price_usd
    FROM query_5187697 -- Oracle Price (1 sUSDS -> USDS)

)
, idle_usds_sum as (
    SELECT ts
        , hash
        , 'USDS' as token
        , SUM(value) as value
    FROM preminted_usds_flows
    GROUP BY 1, 2
    UNION ALL
    SELECT ts
        , hash
        , 'sUSDS' as token
        , SUM(value) * sop.price_usd as value
    FROM preminted_susds_flows as psf
        JOIN susds_oracle_price  as sop on date(psf.ts) = sop.dt 
    GROUP BY 1, 2, sop.price_usd
)
-- These transactions relate to Escrowed USDS in Arbitrum on Mainnet. They are preminted USDS and should be decreased 
-- from USDS circulation, only affecting the balance sheet.
, idle_usds_txs as (
    select
        ts,
        hash,
        211211 as account_id, -- Preminted USDS (Liabilities)
        -value as value
    from idle_usds_sum
    where value != 0
    UNION ALL
    select
        ts,
        hash,
        134112 as account_id,  -- Preminted Yielding USDS (Assets)
        -value as value
    from idle_usds_sum
    where value != 0
)

, spark_revenues as (
    SELECT dt as ts, TRY_CAST(null as VARBINARY) as hash
    , CASE 
        WHEN source = 'spark' THEN 34110
        WHEN source = 'grove' THEN 34111
    END as account_id -- SubDAO Revenues
    , (tvl * weighted_apy / 365) as value, 'USDS' as token, 'Star Agents Est. Revenue' as descriptor
    , CASE 
        WHEN source = 'spark' then 'ALLOCATOR-SPARK-A'
        WHEN source = 'grove' then 'ALLOCATOR-BLOOM-A'
    END
        as ilk
    FROM dune.steakhouse.result_spark_assets
    where dt >= timestamp'2025-07-01 00:00'
)
, spark_unioned as (
    SELECT ts, hash, account_id
        , value, token, descriptor, ilk
    FROM spark_revenues
    union all

    SELECT ts, hash, 21121 as account_id -- USDS (Non yielding)
        , -value, token, descriptor, ilk
    FROM spark_revenues
)
select
    ts
    , hash
    , account_id
    , value
    , 'USDS' as token
    , 'SSR Expenses' as descriptor
    , cast(null as varchar) as ilk
from ssr_expenses

union all

select
    ts
    , hash
    , account_id
    , value
    , 'USDS' as token
    , 'SSR Flows' as descriptor
    , cast(null as varchar) as ilk
from ssr_flows

union all

select ts
    , hash
    , account_id
    , value
    , 'SKY' as token
    , 'SKY Pause Proxy Trxns' as descriptor
    , cast(null as varchar) as ilk
from sky_incentives

union all

select ts
    , hash
    , account_id
    , value
    , 'USDS' as token
    , descriptor
    , cast(null as varchar) as ilk
from usds_flows

UNION ALL

SELECT ts
    , hash
    , account_id
    , value
    , 'USDS' as token
    , 'Sky Burns' as descriptor
    , cast(null as varchar) as ilk
FROM sky_burns

UNION ALL

SELECT ts
    , hash
    , account_id
    , value
    , 'USDS' as token
    , 'Idle USDS' as descriptor
    , cast(null as varchar) as ilk
FROM idle_usds_txs

UNION ALL

SELECT ts
    , hash
    , account_id
    , value
    , 'USDS' as token
    , 'Sky Staking Rewards' as descriptor
    , cast(null as varchar) as ilk
FROM sky_staking

UNION ALL

SELECT ts
    , hash
    , account_id
    , value
    , 'USDS' as token
    , descriptor
    , cast(null as varchar) as ilk
FROM subdao_surplus_txns

-- UNION ALL
-- SELECT ts
--     , hash
--     , account_id
--     , value
--     , token
--     , descriptor
--     , ilk
-- FROM spark_unioned