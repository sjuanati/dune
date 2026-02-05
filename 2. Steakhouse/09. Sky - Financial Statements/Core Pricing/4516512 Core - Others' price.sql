/*
-- @title: Core - Others' price
-- @description: Handles prices derived from shares prices for Other tokens (derived from custom based solutions).
-- @author: Steakhouse Financial
-- @notes: when adding new prices, remember updating matview query_4583090
-- @version:
    - 1.0 - 2025-01-15 - Initial version
    - 2.0 - 2025-03-12 - Add comments
    - 3.0 - 2025-04-25 - Removed sUSDS cslc (already created in oracle queries incl.sUSDC both for multiple chains)
                       - Added srUSD
    - 4.0 - 2025-04-29 - Added price backfilling (to correctly keep the calculated_price_usd in the final token price query)
    - 5.0 - 2025-06-04 - Added OUSG
    - 6.0 - 2025-06-09 - Added Compound's cUSDC, cUSDT & cDAI
    - 7.0 - 2025-08-13 - Removed OUSG and replaced by Oracle calculation in query_4534624
*/

with
    srusd as (
        select
            date(evt_block_time) as dt,
            'ethereum' as blockchain,
            'srUSD' as symbol,
            0x738d1115b90efa71ae468f1287fc864775e23a31 as token_address,
            max_by(rusd / cast(srusd as double), evt_block_time) as price_usd
        from (
            select
                evt_block_time,
                burnAmount / 1e18 as rusd, -- rUSD burnt
                mintAmount / 1e18 as srusd -- srUSD minted
            from reservoir_protocol_ethereum.savingmodule_evt_mint
            where mintAmount > 1e10
            union all
            select
                evt_block_time,
                redeemAmount / 1e18 as rusd, -- rUSD redeemed
                burnAmount / 1e18 as srudd   -- srUSD burnt
            from reservoir_protocol_ethereum.savingmodule_evt_redeem
            where burnAmount > 1e10
        )
        group by 1
    ),
    compound as (
        select
            date(evt_block_time) as dt,
            'ethereum' as blockchain,
            case
                when contract_address = 0x39aa39c021dfbae8fac545936693ac917d5e7563 then 'cUSDC'
                when contract_address = 0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9 then 'cUSDT'
                when contract_address = 0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643 then 'cDAI'
            end as symbol,
            contract_address as token_address,
            max_by(token / cast(cToken as double), evt_block_time) as price_usd
        from (
            select
                evt_block_time,
                contract_address,
                mintAmount / if(contract_address = 0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643, 1e18, 1e6) as token,
                mintTokens / 1e8 as cToken
            from (
                select evt_block_time, contract_address, mintAmount, mintTokens from compound_v2_ethereum.cerc20_evt_mint
                union all
                select evt_block_time, contract_address, mintAmount, mintTokens from compound_v2_ethereum.cerc20delegator_evt_mint
            )
            where contract_address in (
                0x39aa39c021dfbae8fac545936693ac917d5e7563, -- cUSDC
                0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9, -- cUSDT
                0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643) -- cDAI
              and mintAmount > 1e4
            union all
            select
                evt_block_time,
                contract_address,
                redeemAmount / if(contract_address = 0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643, 1e18, 1e6) as token,
                redeemTokens / 1e8 as cToken
            from (
                select evt_block_time, contract_address, redeemAmount, redeemTokens from compound_v2_ethereum.cerc20_evt_redeem
                union all
                select evt_block_time, contract_address, redeemAmount, redeemTokens from compound_v2_ethereum.cerc20delegator_evt_redeem
            )
            where contract_address in (
                0x39aa39c021dfbae8fac545936693ac917d5e7563, -- cUSDC
                0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9, -- cUSDT
                0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643) -- cDAI
              and redeemAmount > 1e4
        )
        group by 1,3,4
    ),
    sdai as (
        select
            date(call_block_time) as dt,
            'ethereum' as blockchain, 
            'sDAI' as symbol,
            0x83F20F44975D03b1b09e64809B757c47f942BEeA as token_address,
            max_by(output_tmp * 1e-27, call_block_time) as price_usd
        from maker_ethereum.pot_call_drip
        where call_block_number >= 8928300 -- 2019-11-13 19:53
        group by 1
    ),
    usyc as (
        select
            date(evt_block_time) as dt,
            'ethereum' as blockchain,
            'USYC' as symbol,
            0x136471a34f6ef19fe571effc1ca711fdb8e49f2b as token_address,
            max_by(price / 1e8, date(evt_block_time)) as price_usd
        from hashnote_ethereum.oracle_evt_balancereported
        --from hashnote_ethereum.YieldTokenAggregator_evt_BalanceReported -- table name was changed
        group by 1, 2, 3, 4
    ),
    wusdm as (
        select
            date(evt_block_time) as dt,
            'ethereum' as blockchain,
            'wUSDM' as symbol,
            0x57F5E098CaD7A3D1Eed53991D4d66C45C9AF7812 as token_address,
            max_by(value / 1e18, evt_block_time) as price_usd
        from mountain_ethereum.USDM_evt_RewardMultiplier
        group by 1
    ),
    pricing_all as (
        select * from srusd
        union all
        select * from compound
        union all
        selecT * from sdai
        union all
        select * from usyc
        union all
        select * from wusdm
    ),
    series as (
        select s.dt, p.*
        from (select blockchain, token_address, symbol, min(dt) as start_date from pricing_all group by 1,2,3) as p
        cross join unnest(sequence(p.start_date, current_date, interval '1' day)) as s(dt)
    ),
    pricing_backfill as (
        select
            dt,
            blockchain,
            s.symbol,
            token_address,
            coalesce(
                p.price_usd,
                last_value(p.price_usd) ignore nulls over (partition by blockchain, token_address order by dt rows between unbounded preceding and current row)
            ) as price_usd
        from series s
        left join pricing_all p using (dt, blockchain, token_address)
    )

select * from pricing_backfill order by dt desc
