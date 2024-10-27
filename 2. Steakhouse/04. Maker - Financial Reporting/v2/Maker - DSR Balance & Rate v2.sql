/*
-- @title: DSR Balance & Rate
-- @author: Steakhouse Financial
-- @description: Displays the latest 12-month trends in DAI+USDS balances within the DAI Savings Rate (DSR) and Sky Savings Rate (SSR) systems
                 and their corresponding rates
-- @notes: N/A
-- @version:
    - 1.0 - 2023-07-12 - Initial version
    - 2.0 - 2024-06-04 - Added header + code refactoring
    - 3.0 - 2024-10-08 - Added USDS and SKY Savings Rate
    - 4.0 - 2024-10-10 - Correctly identify SSR balance.
    - 5.0 - 2024-10-15 - Added in surplus buffer in the calculation.
*/

with
    deltas as (
        -- DAI
        select
            dst as address,
            date(evt_block_time) as dt,
            cast(wad as double) as delta
        from maker_ethereum.DAI_evt_Transfer
        union all
        select
            src as address,
            date(evt_block_time) as dt,
            -cast(wad as double) as delta
        from maker_ethereum.DAI_evt_Transfer
        union all
        select
            dst as address,
            date(call_block_time) as dt,
            cast(rad as double) / 1e27 as delta
        from maker_ethereum.VAT_call_move
        where call_success
          and dst IN (0x197e90f9fad81970ba7976f33cbd77088e5d7cf7)
        union all
        select
            src as address,
            date(call_block_time) as dt,
            -cast(rad as double) / 1e27 as delta
        from maker_ethereum.VAT_call_move
        where call_success
          and src IN (0x197e90f9fad81970ba7976f33cbd77088e5d7cf7)
        union all
        select
            v as address,
            date(call_block_time) as dt,
            cast(rad as double) / 1e27 as delta
        from maker_ethereum.VAT_call_suck
        where call_success
          and v IN (0x197e90f9fad81970ba7976f33cbd77088e5d7cf7)
        -- USDS
        union all
        select
            "to" AS address,
            date(evt_block_time) AS dt,
            cast(value as double) AS delta
        FROM sky_ethereum.USDS_evt_Transfer
        UNION ALL
        SELECT
            "from" AS address,
            DATE(evt_block_time) AS dt,
            -cast(value as double) AS delta
        FROM sky_ethereum.USDS_evt_Transfer
        
        UNION ALL
        
        -- Surplus Buffer
        SELECT
            0x12 as address,
            date(period) as dt,
            protocol_surplus * 1e18 as delta
        FROM query_3690490
        WHERE period >= DATE'2024-09-01'
    ),
    deltas2 as (
        select
            case address
                when 0xa3931d71877c0e7a3148cb7eb4463524fec27fbd then 'SSR' -- SKY Saving Rate (SSR)
                when 0x197e90f9fad81970ba7976f33cbd77088e5d7cf7 then 'DSR' -- DAI Saving Rate (DSR)
                else 'non-savings'
            end as wallet,
            address,
            deltas.dt,
            deltas.delta
        from deltas
        where address not in (
        0x0000000000000000000000000000000000000000, -- Burn Address
        0xf6e72db5454dd049d0788e411b06cfaf16853042  -- LitePSM USDC A
        )     
        union all
        select
            wallet,
            NULL as address,
            dt,
            0 as delta
        from (select 'SSR' as wallet UNION select 'DSR' union select 'non-savings')
        cross join unnest(sequence(date('2020-01-01'), current_date, interval '1' day)) as t(dt)
    ),
    grouped as (
        select
            dt,
            sum(case when wallet = 'SSR' then delta else 0 end) as ssr_delta,
            sum(case when wallet = 'DSR' then delta else 0 end) as dsr_delta,
            sum(case when wallet = 'non-savings' then delta else 0 end) as non_savings_delta
        from deltas2
        group by 1
    ),
    dsr as (
        select
            date(call_block_time) as dt,
            power((output_0 / 1e27), 365 * 24 * 60 * 60) - 1 as dsr_rate
        from (
            select
                call_block_time,
                output_0,
                lag(output_0) over (order by call_block_time) as lag_output_0
            from maker_ethereum.pot_call_dsr
            where call_success
        )
        where output_0 <> coalesce(lag_output_0, cast(0 as uint256))
    ),
    ssr as (
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
    ),
    grouped_w_dsr as (
        select
            *,
            sum(case when dsr_rate is not NULL then 1 else 0 end) over (order by dt) as dsr_rate_grp,
            sum(case when ssr_rate is not NULL then 1 else 0 end) over (order by dt) as ssr_rate_grp
        from grouped
        left join dsr using (dt)
        left join ssr using (dt)
    ),
    balances as (
    select
        dt,
        sum(ssr_delta) over (order by dt) / 1e18 as ssr_balance,
        sum(dsr_delta) over (order by dt) / 1e18 as dsr_balance,
        sum(ssr_delta + dsr_delta) over (order by dt) / 1e18 as savings_balance,
        sum(non_savings_delta) over (order by dt) / 1e18 as non_savings_balance,
        sum(ssr_delta + dsr_delta + non_savings_delta) over (order by dt) / 1e18 as total_balance,
        max(dsr_rate) over (partition by dsr_rate_grp) as dsr_rate,
        max(ssr_rate) over (partition by ssr_rate_grp) as ssr_rate
        from grouped_w_dsr
    )

select * from balances
where dt > current_date - interval '14' month
order by dt desc nulls first