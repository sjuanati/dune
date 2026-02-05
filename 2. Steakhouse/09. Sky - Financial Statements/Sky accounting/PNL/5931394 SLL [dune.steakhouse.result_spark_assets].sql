/*
-- @title: Spark Liquidity Layer
-- @description: Tracks the Spark Liquidity Layer asset value
-- @author: Steakhouse Financial
-- @notes: N/A
-- @data validation: compare below query vs. https://debank.com/profile/0x1601843c5e9bc251a3272907010afa41fa18347e
--                   https://data.spark.fi/spark-liquidity-layer
-- @version:
    - 1.0 - 2025-08-27 - Initial release
    - 2.0 - 2025-10-08 - Official Release
    - 3.0 - 2025-10-19 - Add RWA integration (track interest flows from BUIDL-I, USTB, JTRSY) 
*/
WITH tokens as (
    SELECT token_address as contract_address, blockchain, symbol, decimals
    FROM dune.steakhouse.result_token_info
    where blockchain in ('ethereum', 'base', 'optimism', 'unichain', 'arbitrum', 'avalanche_c')
        and token_address in (
            0x4c9edd5852cd905f086c759e8383e09bff1e68b3 -- USDe
            , 0x9d39a5de30e57443bff2a8307a4256c8797a3497 -- sUSDe
            , 0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b -- syrupUSDC
            , 0x09aa30b182488f769a9824f15e6ce58591da4781 -- aEthLidoUSDS
            , 0x4dedf26112b3ec8ec46e7e31ea5e123490b05b8b -- spDAI (Spark)
            , 0x377c3bd93f2a2984e1e7be6a5c22c525ed4a4815 -- spUSDC
            , 0xc02ab1a5eaa8d1b114ef786d9bde108cd4364359 -- spUSDT
            , 0xe7df13b8e3d6740fe17cbe928c7334243d86c92f -- spUSDS
            , 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 -- USDC
            , 0xdac17f958d2ee523a2206206994597c13d831ec7 -- USDT
            , 0xc139190f447e929f090edeb554d95abb8b18ac1c -- USDtb
            , 0x7bfa7c4f149e7415b73bdedfe609237e29cbf34a -- sparkUSDC
            , 0xe41a0583334f0dc4e023acd0bfef3667f6fe0597 -- sparkUSDS
            , 0x73e65dbd630f90604062f6e02fab9138e713edd9 -- spDAI (Morpho)
            , 0x779224df1c756b4edd899854f32a53e8c2b2ce5d -- spPYUSD
            , 0x4e65fe4dba92790696d040ac24aa414708f5c0ab -- abUSDC
            , 0x625e7708f30ca75bfd92586e17077590c60eb4cd -- aAvaUSDC
            , 0x56a76b428244a50513ec81e225a293d128fd581d -- sparkUSDCbc
            , 0xf62e339f21d8018940f188f6987bcdf02a849619 -- fsUSDS
            , 0xdC035D45d973E3EC169d2276DDab16f1e407384F -- USDS
            , 0x6c3ea9036406852006290770bedfcaba0e23a0e8 -- PYUSD

            , 0x43415eb6ff9db7e26a15b704e7a3edce97d31c4e -- USTB 
            , 0x6a9da2d710bb9b700acde7cb81f10f1ff8c89041 -- BUIDL-I 
            
            , 0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85 -- USDC
            , 0x078D782b760474a361dDA0AF3839290b0EF57AD6 -- USDC
            , 0xaf88d065e77c8cC2239327C5EDb3A432268e5831 -- USDC
            , 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913 -- USDC
            , 0x6491c05A82219b8D1479057361ff1654749b876b -- USDS
            , 0x7E10036Acc4B56d4dFCa3b77810356CE52313F9C -- USDS
            , 0x4F13a96EC5C4Cf34e442b46Bbd98a0791F20edC3 -- USDS (Optimism)
            , 0x820C137fa70C8691f0e44Dc420a5e53c168921Dc -- USDS

            , 0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD -- sUSDS (Ethereum)
            , 0x5875eEE11Cf8398102FdAd704C9E96607675467a -- sUSDS (Base)
            , 0xa06b10db9f390990364a3984c04fadf1c13691b5 -- sUSDS (Unichain)
            , 0xb5b2dc7fd34c249f4be7fb1fcea07950784229e0 -- sUSDS (Optimism)
            , 0xddb46999f8891663a8f2828d25298f70416d7610 -- sUSDS (Arbitrum)
        )
    UNION ALL
    SELECT token_address as contract_address, blockchain, symbol, decimals
    FROM dune.steakhouse.result_token_info
    where blockchain in ('ethereum')
        and token_address = 0x8c213ee79581ff4984583c6a801e5263418c4b86 -- JTRSY 
)
, non_yield_assets as (
    SELECT date(ts) as dt, contract_address, symbol, SUM(value / POWER(10, decimals)) as value
    FROM (
        SELECT evt_block_time as ts, contract_address, "from" as owner, -value as value
        FROM erc20_ethereum.evt_transfer
        WHERE evt_block_time >= timestamp'2023-01-01 00:00'
        UNION ALL
        SELECT evt_block_time as ts, contract_address, "to" as owner, value
        FROM erc20_ethereum.evt_transfer
        WHERE evt_block_time >= timestamp'2023-01-01 00:00'
        UNION ALL
        SELECT evt_block_time as ts, contract_address, "from" as owner, -value as value
        FROM erc20_base.evt_transfer
        WHERE evt_block_time >= timestamp'2023-01-01 00:00'
        UNION ALL
        SELECT evt_block_time as ts, contract_address, "to" as owner, value
        FROM erc20_base.evt_transfer
        WHERE evt_block_time >= timestamp'2023-01-01 00:00'
        UNION ALL
        SELECT evt_block_time as ts, contract_address, "from" as owner, -value as value
        FROM erc20_arbitrum.evt_transfer
        WHERE evt_block_time >= timestamp'2023-01-01 00:00'
        UNION ALL
        SELECT evt_block_time as ts, contract_address, "to" as owner, value
        FROM erc20_arbitrum.evt_transfer
        WHERE evt_block_time >= timestamp'2023-01-01 00:00'
        UNION ALL
        SELECT evt_block_time as ts, contract_address, "from" as owner, -value as value
        FROM erc20_unichain.evt_transfer
        WHERE evt_block_time >= timestamp'2023-01-01 00:00'
        UNION ALL
        SELECT evt_block_time as ts, contract_address, "to" as owner, value
        FROM erc20_unichain.evt_transfer
        WHERE evt_block_time >= timestamp'2023-01-01 00:00'
        UNION ALL
        SELECT evt_block_time as ts, contract_address, "from" as owner, -value as value
        FROM erc20_optimism.evt_transfer
        WHERE evt_block_time >= timestamp'2023-01-01 00:00'
        UNION ALL
        SELECT evt_block_time as ts, contract_address, "to" as owner, value
        FROM erc20_optimism.evt_transfer
        WHERE evt_block_time >= timestamp'2023-01-01 00:00'
    ) join tokens using (contract_address)
    WHERE contract_address in (
        0xdAC17F958D2ee523a2206206994597C13D831ec7 -- USDT
        , 0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85 -- USDC (Optimism)
        , 0x078D782b760474a361dDA0AF3839290b0EF57AD6 -- USDC (Unichain)
        , 0xaf88d065e77c8cC2239327C5EDb3A432268e5831 -- USDC (Arbitrum)
        , 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913 -- USDC (Base)
        , 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 -- USDC (Ethereum)
        , 0xdC035D45d973E3EC169d2276DDab16f1e407384F -- USDS (Ethereum)
        , 0x6491c05A82219b8D1479057361ff1654749b876b -- USDS (Arbitrum)
        , 0x7E10036Acc4B56d4dFCa3b77810356CE52313F9C -- USDS (Unichain)
        , 0x4F13a96EC5C4Cf34e442b46Bbd98a0791F20edC3 -- USDS (Optimism)
        , 0x820C137fa70C8691f0e44Dc420a5e53c168921Dc -- USDS (Base)
        , 0x6c3ea9036406852006290770bedfcaba0e23a0e8 -- PYUSD
    )
    and owner IN (
        0x876664f0c9Ff24D1aa355Ce9f1680AE1A5bf36fB -- Optimism ALM Proxy
        , 0x345e368fccd62266b3f5f37c9a131fd1c39f5869 -- Unichain ALM Proxy
        , 0x92afd6F2385a90e44da3a8B60fe36f6cBe1D8709 -- Arbitrum ALM Proxy
        , 0x2917956eFF0B5eaF030abDB4EF4296DF775009cA -- Base ALM Proxy
        , 0x1601843c5E9bC251A3272907010AFa41Fa18347E -- Base PSM3
        , 0x7b42Ed932f26509465F7cE3FAF76FfCe1275312f -- Unichain PSM3
        , 0x2B05F8e1cACC6974fD79A673a341Fe1f58d27266 -- Arbitrum PSM3
        , 0xe0F9978b907853F354d79188A3dEfbD41978af62 -- Optimism PSM3
    )
    GROUP BY 1, 2, 3
)
, sky as (
    SELECT date(ts) as dt, contract_address, symbol, SUM(value / POWER(10, decimals)) as value
    FROM (
        SELECT evt_block_time as ts, contract_address, "from" as owner, -value as value
        FROM erc20_ethereum.evt_transfer
        WHERE evt_block_time >= timestamp'2023-01-01 00:00'
        UNION ALL
        SELECT evt_block_time as ts, contract_address, "to" as owner, value
        FROM erc20_ethereum.evt_transfer
        WHERE evt_block_time >= timestamp'2023-01-01 00:00'
        UNION ALL
        SELECT evt_block_time as ts, contract_address, "from" as owner, -value as value
        FROM erc20_base.evt_transfer
        WHERE evt_block_time >= timestamp'2023-01-01 00:00'
        UNION ALL
        SELECT evt_block_time as ts, contract_address, "to" as owner, value
        FROM erc20_base.evt_transfer
        WHERE evt_block_time >= timestamp'2023-01-01 00:00'
        UNION ALL
        SELECT evt_block_time as ts, contract_address, "from" as owner, -value as value
        FROM erc20_arbitrum.evt_transfer
        WHERE evt_block_time >= timestamp'2023-01-01 00:00'
        UNION ALL
        SELECT evt_block_time as ts, contract_address, "to" as owner, value
        FROM erc20_arbitrum.evt_transfer
        WHERE evt_block_time >= timestamp'2023-01-01 00:00'
        UNION ALL
        SELECT evt_block_time as ts, contract_address, "from" as owner, -value as value
        FROM erc20_unichain.evt_transfer
        WHERE evt_block_time >= timestamp'2023-01-01 00:00'
        UNION ALL
        SELECT evt_block_time as ts, contract_address, "to" as owner, value
        FROM erc20_unichain.evt_transfer
        WHERE evt_block_time >= timestamp'2023-01-01 00:00'
        UNION ALL
        SELECT evt_block_time as ts, contract_address, "from" as owner, -value as value
        FROM erc20_optimism.evt_transfer
        WHERE evt_block_time >= timestamp'2023-01-01 00:00'
        UNION ALL
        SELECT evt_block_time as ts, contract_address, "to" as owner, value
        FROM erc20_optimism.evt_transfer
        WHERE evt_block_time >= timestamp'2023-01-01 00:00'
    ) join tokens using (contract_address)
    WHERE contract_address in (
        0x5875eEE11Cf8398102FdAd704C9E96607675467a -- sUSDS (Base)
        , 0xa06b10db9f390990364a3984c04fadf1c13691b5 -- sUSDS (Unichain)
        , 0xb5b2dc7fd34c249f4be7fb1fcea07950784229e0 -- sUSDS (Optimism)
        , 0xddb46999f8891663a8f2828d25298f70416d7610 -- sUSDS (Arbitrum)
    )
    and owner IN (
        0x876664f0c9Ff24D1aa355Ce9f1680AE1A5bf36fB -- Optimism ALM Proxy
        , 0x345e368fccd62266b3f5f37c9a131fd1c39f5869 -- Unichain ALM Proxy
        , 0x92afd6F2385a90e44da3a8B60fe36f6cBe1D8709 -- Arbitrum ALM Proxy
        , 0x2917956eFF0B5eaF030abDB4EF4296DF775009cA -- Base ALM Proxy
        , 0x1601843c5E9bC251A3272907010AFa41Fa18347E -- Base PSM3
        , 0x7b42Ed932f26509465F7cE3FAF76FfCe1275312f -- Unichain PSM3
        , 0x2B05F8e1cACC6974fD79A673a341Fe1f58d27266 -- Arbitrum PSM3
        , 0xe0F9978b907853F354d79188A3dEfbD41978af62 -- Optimism PSM3
    )
    GROUP BY 1, 2, 3
)
, syrup as (
    SELECT ts, contract_address, symbol, shares / POWER(10, decimals) as shares, assets / POWER(10, decimals) as assets, 'syrup' as protocol
        , case owner when  0x1601843c5E9bC251A3272907010AFa41Fa18347E then 'spark' when 0xb6dD7ae22C9922AFEe0642f9Ac13e58633f715A2 then 'obex' end as source
    FROM (
        SELECT evt_block_time as ts, contract_address, owner_ as owner, shares_ as shares, assets_ as assets
        FROM maplefinance_v2_ethereum.pool_v2_evt_deposit
        UNION ALL
        SELECT evt_block_time, contract_address, receiver_ as owner, -shares_ as shares, -assets_ as assets
        FROM maplefinance_v2_ethereum.pool_v2_evt_withdraw
    ) join tokens using (contract_address)
    WHERE owner in (
        0x1601843c5E9bC251A3272907010AFa41Fa18347E -- ALMProxy
        , 0xb6dD7ae22C9922AFEe0642f9Ac13e58633f715A2 -- OBEX
    )
)
, ethena_withdrawn_pending_txs as (

    SELECT evt_block_time as ts, contract_address, owner, shares, assets
    FROM ethena_labs_ethereum.stakedusdev2_evt_withdraw
    WHERE owner = 0x1601843c5E9bC251A3272907010AFa41Fa18347E -- ALMProxy
)
, ethena as (
    SELECT ts, contract_address, symbol, shares / POWER(10, decimals) as shares, assets / POWER(10, decimals) as assets
    FROM (
        SELECT evt_block_time as ts, contract_address, owner, shares, assets
        FROM ethena_labs_ethereum.stakedusdev2_evt_deposit        
        union all
        SELECT ts, contract_address, owner, -shares, -assets
        FROM ethena_withdrawn_pending_txs
    ) join tokens using (contract_address)
    WHERE owner = 0x1601843c5E9bC251A3272907010AFa41Fa18347E -- ALMProxy
    UNION ALL
    SELECT ts, contract_address, symbol, value / POWER(10, decimals) as value, value / POWER(10, decimals) as assets
    -- SELECT ts, contract_address, symbol, shares / POWER(10, s.decimals) as shares, assets / POWER(10, c.decimals) as assets
    FROM (
        -- SELECT evt_block_time as ts, contract_address, collateral_asset, benefactor as owner, usde_amount as shares, collateral_amount as assets
        -- FROM ethena_labs_ethereum.ethenaminting2_evt_mint
        -- UNION ALL
        -- SELECT evt_block_time as ts, contract_address, collateral_asset, benefactor as owner, -usde_amount as shares, -collateral_amount as assets
        -- FROM ethena_labs_ethereum.ethenaminting2_evt_redeem
        -- UNION ALL
        SELECT evt_block_time as ts, contract_address, "to" as owner, value
        FROM ethena_labs_ethereum.usde_evt_transfer
        UNION ALL
        SELECT evt_block_time as ts, contract_address, "from" as owner, -value
        FROM ethena_labs_ethereum.usde_evt_transfer
        UNION ALL
        
        -- Plug for pending USDe in contract in Silo contract
        SELECT ts, 0x4c9edd5852cd905f086c759e8383e09bff1e68b3, owner, assets
        FROM ethena_withdrawn_pending_txs
        UNION ALL
        SELECT ts + interval'7' day, 0x4c9edd5852cd905f086c759e8383e09bff1e68b3, owner, -assets
        FROM ethena_withdrawn_pending_txs
    ) as m
        -- left join tokens as c on m.collateral_asset = c.contract_address
        -- left join tokens as s on s.contract_address = 0x4c9edd5852cd905f086c759e8383e09bff1e68b3 -- USDe
        join tokens using (contract_address)
    WHERE owner = 0x1601843c5E9bC251A3272907010AFa41Fa18347E -- ALMProxy
)
, spark as (
    select ts, contract_address, symbol, shares / POWER(10, decimals) as shares, assets / POWER(10, decimals) as assets
    FROM (
        SELECT evt_block_time as ts, contract_address, onBehalfOf as owner, (value/1E0 - balanceIncrease/1E0) / (index / POWER(10, 27)) as shares, (value/1E0 - balanceIncrease/1E0) as assets
        FROM spark_protocol_ethereum.atoken_evt_mint
        UNION ALL
        SELECT evt_block_time as ts, contract_address, target as owner, -(value/1E0 + balanceIncrease/1E0) / (index / POWER(10, 27)) as shares, -(value/1E0 + balanceIncrease/1E0) as assets
        FROM spark_protocol_ethereum.atoken_evt_burn
    ) join tokens using (contract_address)
    WHERE owner IN (0x1601843c5E9bC251A3272907010AFa41Fa18347E, 0x1601843c5e9bc251a3272907010afa41fa18347e) -- ALMProxy
)
, aave as (
    select ts, contract_address, symbol, shares / POWER(10, decimals) as shares, assets / POWER(10, decimals) as assets
    FROM (
        SELECT evt_block_time as ts, contract_address, onBehalfOf as owner, (value/1E0 - balanceIncrease/1E0) / (index / POWER(10, 27)) as shares, (value/1E0 - balanceIncrease/1E0) as assets
        FROM aave_v3_lido_ethereum.atoken_evt_mint
        UNION ALL
        SELECT evt_block_time as ts, contract_address, target as owner, -(value/1E0 + balanceIncrease/1E0) / (index / POWER(10, 27)) as shares, -(value/1E0 + balanceIncrease/1E0) as assets
        FROM aave_v3_lido_ethereum.atoken_evt_burn
        UNION ALL
        SELECT evt_block_time as ts, contract_address, onBehalfOf as owner, (value/1E0 - balanceIncrease/1E0) / (index / POWER(10, 27)) as shares, (value/1E0 - balanceIncrease/1E0) as assets
        FROM aave_v3_multichain.atoken_evt_mint
        UNION ALL
        SELECT evt_block_time as ts, contract_address, target as owner, -(value/1E0 + balanceIncrease/1E0) / (index / POWER(10, 27)) as shares, -(value/1E0 + balanceIncrease/1E0) as assets
        FROM aave_v3_multichain.atoken_evt_burn
    ) join tokens using (contract_address)
    WHERE owner in (0x1601843c5E9bC251A3272907010AFa41Fa18347E, 0x2917956eff0b5eaf030abdb4ef4296df775009ca) -- ALMProxy
)
, morpho as (
    SELECT a.ts, contract_address, t.symbol, a.shares / POWER (10, t.decimals) as shares, a.assets / POWER(10, v.token_decimals) as assets
    FROM (
        SELECT evt_block_time as ts, contract_address, owner, shares, assets
        FROM metamorpho_vaults_multichain.metamorpho_evt_deposit
        UNION ALL
        SELECT evt_block_time as ts, contract_address, owner, -shares, -assets
        FROM metamorpho_vaults_multichain.metamorpho_evt_withdraw
        UNION ALL
        SELECT evt_block_time as ts, contract_address, owner, shares, assets
        FROM metamorpho_vaults_multichain.metamorphov1_1_evt_deposit
        UNION ALL
        SELECT evt_block_time as ts, contract_address, owner, -shares, -assets
        FROM metamorpho_vaults_multichain.metamorphov1_1_evt_withdraw
        UNION ALL
        SELECT t.evt_block_time as ts, t.contract_address, "to" as owner, feeShares as shares, 0 as assets
        FROM metamorpho_vaults_multichain.metamorphov1_1_evt_transfer as t
            JOIN metamorpho_vaults_multichain.metamorphov1_1_evt_accrueinterest as i
            ON t.evt_tx_hash = i.evt_tx_hash and t.contract_address = i.contract_address and t.evt_index = i.evt_index - 1 and t.value = i.feeShares
        WHERE t."from" = 0x0000000000000000000000000000000000000000
        UNION ALL
        SELECT t.evt_block_time as ts, t.contract_address, "to" as owner, feeShares, 0 as assets
        FROM metamorpho_vaults_multichain.metamorpho_evt_transfer as t
            JOIN metamorpho_vaults_multichain.metamorpho_evt_accrueinterest as i
            ON t.evt_tx_hash = i.evt_tx_hash and t.contract_address = i.contract_address and t.evt_index = i.evt_index - 1 and t.value = i.feeShares
        WHERE t."from" = 0x0000000000000000000000000000000000000000
    ) as a 
        join tokens as t using (contract_address)
        join dune.steakhouse.result_morpho_vaults as v on contract_address = v.vault_address
    WHERE owner IN (0x1601843c5E9bC251A3272907010AFa41Fa18347E -- ALMProxy
    , 0x2917956eFF0B5eaF030abDB4EF4296DF775009cA) -- Base ALMProxy
)
, curve_tokens(contract_address, symbol, token0_address, token1_address) as (
    VALUES
    (0xA632D59b9B804a956BfaA9b48Af3A1b74808FC1f, 'PYUSDUSDS', 0x6c3ea9036406852006290770bedfcaba0e23a0e8, 0xdC035D45d973E3EC169d2276DDab16f1e407384F),
    (0x00836Fe54625BE242BcFA286207795405ca4fD10, 'sUSDSUSDT', 0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD, 0xdAC17F958D2ee523a2206206994597C13D831ec7)
)
, curvefi_withdraws as (
    SELECT block_time as ts
        , e.contract_address
        , c.symbol
        , bytearray_substring(topic1, 1 + 12, 20) as provider
        -- , bytearray_to_int256(bytearray_substring(data, 1 + 32 * 7, 32)) as amt
        , bytearray_to_int256(bytearray_substring(data, 1, 32)) as token_id
        , bytearray_to_int256(bytearray_substring(data, 1 + 32 * 1, 32)) as token_amount
        , bytearray_to_int256(bytearray_substring(data, 1 + 32 * 2, 32)) as coin_amount
        , bytearray_to_int256(bytearray_substring(data, 1 + 32 * 3, 32)) as token_supply
        , t_0.decimals as t0_dec, t_1.decimals as t1_dec
        , tx_hash
    FROM ethereum.logs as e
        JOIN curve_tokens as c on e.contract_address = c.contract_address
        LEFT JOIN tokens as t_0 on t_0.contract_address = c.token0_address 
        LEFT JOIN tokens as t_1 on t_1.contract_address = c.token1_address
    WHERE topic0 = 0x6f48129db1f37ccb9cc5dd7e119cb32750cabdf75b48375d730d26ce3659bbe1 -- Remove Liquidity One
)

, curvefi as (

    SELECT block_time as ts
        , contract_address
        , c.symbol
        , bytearray_to_int256(bytearray_substring(data, 1, 32)) / 1E18 AS shares
        , bytearray_to_int256(bytearray_substring(data, 1, 32)) / 1E18 AS assets
    FROM ethereum.logs as t
        JOIN curve_tokens as c using(contract_address)
    WHERE topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef -- Transfer
    and bytearray_substring(topic2, 1 + 12, 20) = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
    UNION ALL
    SELECT block_time as ts
        , contract_address
        , c.symbol
        , -(bytearray_to_int256(bytearray_substring(data, 1, 32)) / 1E18) AS shares
        , -(bytearray_to_int256(bytearray_substring(data, 1, 32)) / 1E18) AS assets
    FROM ethereum.logs as t
        JOIN curve_tokens as c using(contract_address)
    WHERE topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef -- Transfer
    and bytearray_substring(topic1, 1 + 12, 20) = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
)
, superstate as (
    SELECT DATE(evt_block_time) as time
        , contract_address
        , 'USTB' as symbol
        , (CASE WHEN "to" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E THEN 1 ELSE -1 END) * (value / POWER(10, 6))  as assets
        , 'spark' as source
    FROM superstate_ethereum.ustb_evt_transfer
    WHERE 0x1601843c5E9bC251A3272907010AFa41Fa18347E IN ("from", "to") --  Spark: Liquidity Layer 
    union all

    SELECT DATE(evt_block_time) as time
        , contract_address
        , 'USTB' as symbol
        , (CASE WHEN "to" = 0x491EDFB0B8b608044e227225C715981a30F3A44E THEN 1 ELSE -1 END) * (value / POWER(10, 6))  as assets
        , 'grove' as source
    FROM superstate_ethereum.ustb_evt_transfer
    WHERE 0x491EDFB0B8b608044e227225C715981a30F3A44E IN ("from", "to") --  Grove

)
, janus as (
    SELECT DATE(evt_block_time) as time
        , contract_address
        , 'JTRSY' as symbol
        , (CASE WHEN "to" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E THEN 1 ELSE -1 END) * (value / POWER(10, 6))  as assets
        , 'spark' as source
    FROM erc20_ethereum.evt_transfer
    WHERE contract_address = 0x8c213ee79581ff4984583c6a801e5263418c4b86
    and 0x1601843c5E9bC251A3272907010AFa41Fa18347E IN ("from", "to") --  Spark: Liquidity Layer 
    UNION ALL

    SELECT DATE(evt_block_time) as time
        , contract_address
        , 'JTRSY' as symbol
        , (CASE WHEN "to" = 0x491EDFB0B8b608044e227225C715981a30F3A44E THEN 1 ELSE -1 END) * (value / POWER(10, 6))  as assets
        , 'grove' as source
    FROM erc20_ethereum.evt_transfer
    WHERE contract_address = 0x8c213ee79581ff4984583c6a801e5263418c4b86
    and 0x491EDFB0B8b608044e227225C715981a30F3A44E IN ("from", "to") --  Grove
)
, protocol_activity as (
    SELECT * from syrup
    union all
    SELECT *, 'ethena' as protocol, 'spark' as source from ethena
    union all
    SELECT *, 'spark' as protocol, 'spark' as source from spark
    union all
    SELECT *, 'aave' as protocol, 'spark' as source from aave
    union all
    SELECT *, 'morpho' as protocol, 'spark' as source from morpho
    union all
    SELECT *, value as assets, null as protocol, 'spark' as source from non_yield_assets
    union all
    SELECT *, value as assets, 'sky' as protocol, 'spark' as source from sky
    union all
    SELECT *, 'curve' as protocol, 'spark' as source from curvefi
    union all
    select time, contract_address, symbol, assets, assets as shares, 'superstate' as protocol, source from superstate
    union all
    select time, contract_address, symbol, assets, assets as shares, 'janus' as protocol, source from janus
)
, protocol_pricing as (
    SELECT period as dt, contract_address, supply_index as index, supply_rate
    FROM dune.steakhouse.result_spark_protocol_markets_data as d
        JOIN tokens as t on d.aToken = t.contract_address
    WHERE d.blockchain = 'ethereum'
    UNION ALL
    SELECT period, contract_address, supply_index, supply_rate
    FROM dune.steakhouse.result_aave_v3_markets_data_prime as d
        JOIN tokens as t on d.aToken = t.contract_address
    WHERE d.blockchain = 'ethereum'
    UNION ALL
    SELECT period, contract_address, supply_index, supply_rate
    FROM     dune.steakhouse.result_aave_v3_markets_data_base as d
        JOIN tokens as t on d.aToken = t.contract_address
    WHERE d.blockchain = 'base'
    UNION ALL
    SELECT period, contract_address, supply_index, supply_rate
    FROM     dune.steakhouse.result_aave_v3_markets_data_avalanche as d
        JOIN tokens as t on d.aToken = t.contract_address
    WHERE d.blockchain = 'avalanche_c'
    UNION ALL
    SELECT dt, token_address, 
        CASE WHEN token_address in (0x9d39a5de30e57443bff2a8307a4256c8797a3497) THEN price_usd --sUSDe
        ELSE COALESCE(accounting_price_usd, price_usd)
        END as price_usd, null
    FROM dune.steakhouse.result_token_price as p
        JOIN tokens as t on p.token_address = t.contract_address and p.blockchain = t.blockchain
    WHERE p.token_address not in (
        select distinct atoken
        from dune.steakhouse.result_spark_protocol_markets_data
        union all
        select distinct vault_address
        from dune.steakhouse.result_morpho_vaults_data
        union all
        select distinct atoken
        from dune.steakhouse.result_aave_v3_markets_data_prime
        union all
        select distinct atoken
        from dune.steakhouse.result_aave_v3_markets_data_base
        
    )
    UNION ALL
    SELECT dt, vault_address, share_price_usd, supply_rate
    FROM dune.steakhouse.result_morpho_vaults_data
    where blockchain in ('base', 'ethereum')
)
, indexed_pricing as (
    SELECT dt, contract_address, index
        , ((index / LAG(index) OVER (PARTITION BY contract_address ORDER BY dt)) - 1) * 365 as supply_rate
    FROM protocol_pricing
    WHERE supply_rate is null and 
    contract_address in (
        0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b -- SyrupUSDC
        , 0x5875eEE11Cf8398102FdAd704C9E96607675467a -- sUSDS (Base)
        , 0xa06b10db9f390990364a3984c04fadf1c13691b5 -- sUSDS (Unichain)
        , 0xb5b2dc7fd34c249f4be7fb1fcea07950784229e0 -- sUSDS (Optimism)
        , 0xddb46999f8891663a8f2828d25298f70416d7610 -- sUSDS (Arbitrum)
        , 0x9D39A5DE30e57443BfF2A8307A4256c8797A3497 -- sUSDe
        , 0xf62e339f21d8018940f188f6987bcdf02a849619 -- fsUSDS (Base)
        , 0x43415eb6ff9db7e26a15b704e7a3edce97d31c4e -- USTB 
        , 0x8c213ee79581ff4984583c6a801e5263418c4b86 -- JTRSY 
    )
)
, pricing as (
    SELECT dt, contract_address, 
        CASE 
            WHEN index = 0 or index is null then lag(index) over (partition by contract_address order by dt)
            ELSE index
        END as index, 
        CASE 
            WHEN supply_rate = 0 or supply_rate is null then lag(supply_rate) over (partition by contract_address order by dt)
            ELSE supply_rate
        END as supply_rate
    FROM (
        SELECT dt, contract_address, index, supply_rate
        FROM indexed_pricing
        UNION ALL
        SELECT dt, contract_address, index, supply_rate
        FROM protocol_pricing
        where contract_address not in (
            0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b -- SyrupUSDC
            , 0x5875eEE11Cf8398102FdAd704C9E96607675467a -- sUSDS (Base)
            , 0xa06b10db9f390990364a3984c04fadf1c13691b5 -- sUSDS (Unichain)
            , 0xb5b2dc7fd34c249f4be7fb1fcea07950784229e0 -- sUSDS (Optimism)
            , 0xddb46999f8891663a8f2828d25298f70416d7610 -- sUSDS (Arbitrum)
            , 0x9D39A5DE30e57443BfF2A8307A4256c8797A3497 -- sUSDe
            , 0xf62e339f21d8018940f188f6987bcdf02a849619 -- fsUSDS (Base)
            , 0x43415eb6ff9db7e26a15b704e7a3edce97d31c4e -- USTB
            , 0x8c213ee79581ff4984583c6a801e5263418c4b86 -- JTRSY
        )
        UNION ALL
        SELECT dt, pool_address, 1 as index, apy as supply_rate
        FROM query_5930803 -- LP Curve Prices
    )
)
, date_series as (
    SELECT time as dt, symbol, source, contract_address, protocol
    FROM (
        SELECT contract_address, symbol, source, protocol, date(min(ts)) as min_dt
        FROM protocol_activity
        group by contract_address, symbol, source, protocol
    ), unnest(sequence(
        min_dt, -- Starting Time
        current_date,
        interval '1' day)
    ) as s(time) 
)
, daily_activity as (

    SELECT  dt, symbol, contract_address, index, supply_rate, protocol, source
        , sum(coalesce(shares, 0)) OVER (partition by contract_address, source order by dt) as shares
        , sum(coalesce(assets, 0)) OVER (partition by contract_address, source order by dt) as assets
    FROM date_series
    LEFT JOIN (
        SELECT date(ts) as dt, contract_address, symbol, protocol, source, sum(shares) as shares, sum(assets) as assets
        FROM protocol_activity
        GROUP BY 1, 2, 3, 4, 5
    ) USING (source, contract_address, dt, symbol, protocol)
    LEFT JOIN pricing using (dt, contract_address)
    WHERE dt < current_date
)
, filler as (
    select dt
    from unnest(sequence(date('2024-10-01'), current_date, interval '1' day)) as t(dt)
)
, ssr_updates as (
    select
        date(call_block_time) as dt,
        power((output_0 / 1e27), 365 * 24 * 60 * 60) - 1 as ssr_rate
    from (
        select
            call_block_time,
            output_0,
            lag(output_0) over (order by call_block_time) as lag_output_0
        from sky_ethereum.sUSDS_call_ssr
        where call_success
    )
    where output_0 <> coalesce(lag_output_0, cast(0 as uint256))
    UNION ALL
    SELECT dt, NULL as ssr_rate
    FROM filler
)
, ssr_overtime as (
    SELECT distinct dt, MAX(ssr_rate) OVER (PARTITION BY rate_grp)  + 0.003 as ssr_rate
    FROM (
        SELECT dt, ssr_rate
            , SUM(case when ssr_rate is not null THEN 1 ELSE 0 END) over (order by dt) as rate_grp
        FROM ssr_updates
    ) as ssr
)
-- 
-- select *
-- from protocol_pricing
-- dune.steakhouse.result_token_price 
-- and
-- WHERE
-- dt >= timestamp'2025-09-03 00:00'
-- and dt <= timestamp'2025-09-05 00:00'

SELECT dt, source, SUM(interest) as interest,  SUM(indexed_shares) as tvl,  SUM(interest)/SUM(indexed_shares) as weighted_apy
--     -- SELECT dt, symbol, protocol, supply_rate, interest, indexed_shares
--     -- , SUM(interest) OVER (PARTITION by dt)/SUM(indexed_shares) OVER (PARTITION by dt) as weighted_apy
FROM (
    SELECT a.dt, source, symbol, protocol, supply_rate
        , shares * index as indexed_shares, assets
        , case
            when interest != 0 
                then interest * 365
            when coalesce(supply_rate, 0) >= ssr_rate
                then ssr_rate  * shares * index
            else coalesce(supply_rate, 0)  * shares * index
        end as interest
    FROM (
        SELECT dt, symbol, protocol, supply_rate, 0 as interest, index
            , case when shares > 0 then shares end shares
            , case when assets > 0 then assets end assets
            , source
        FROM daily_activity
        UNION ALL
        SELECT time, symbol, 'blackrock' as protocol, 0 as supply_rate, interest_daily, price as index, outstanding as shares
            , outstanding as assets, 'spark' as source
        FROM dune.steakhouse.result_blackrock_grand_prix_daily_activity
        UNION ALL
        SELECT time, symbol, 'blackrock' as protocol, 0 as supply_rate, interest_daily, price as index, outstanding as shares
            , outstanding as assets, 'grove' as source
        FROM dune.steakhouse.result_blackrock_grove_daily_activity -- Grove Grand Prix
    ) AS a
        JOIN ssr_overtime as s on a.dt = s.dt
)
GROUP BY 1, 2