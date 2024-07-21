/*
-- @title: Maker - DSR Balance & Rate
-- @author: Steakhouse Financial
-- @description: Displays the latest 12-month trends in DAI balances within the Dai Savings Rate (DSR) system and the corresponding DSR rates,
                 showing changes in user engagement and interest yield dynamics
-- @notes: N/A
-- @version:
    - 1.0 - 2024-06-04 - Initial version
*/

with
    deltas as (
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
            0x197e90f9fad81970ba7976f33cbd77088e5d7cf7 as address,
            date(call_block_time) as dt,
            cast(rad as double) / 1e27 as delta
        from maker_ethereum.VAT_call_move
        where call_success
          and dst = 0x197e90f9fad81970ba7976f33cbd77088e5d7cf7
        union all
        select
            0x197e90f9fad81970ba7976f33cbd77088e5d7cf7 as address,
            date(call_block_time) as dt,
            -cast(rad as double) / 1e27 as delta
        from maker_ethereum.VAT_call_move
        where call_success
          and src = 0x197e90f9fad81970ba7976f33cbd77088e5d7cf7
        union all
        select
            0x197e90f9fad81970ba7976f33cbd77088e5d7cf7 as address,
            date(call_block_time) as dt,
            cast(rad as double) / 1e27 as delta
        from maker_ethereum.VAT_call_suck
        where call_success
          and v = 0x197e90f9fad81970ba7976f33cbd77088e5d7cf7
    ),
    deltas2 as (
        select
            case
                when address = 0x197e90f9fad81970ba7976f33cbd77088e5d7cf7 then 'DSR' else 'non-DSR'
            end as wallet,
            address,
            deltas.dt,
            deltas.delta
        from deltas
        where address not in (0x0000000000000000000000000000000000000000) /* burn address */    
        union all
        select
            wallet,
            NULL as address,
            dt,
            0 as delta
        from (select 'DSR' as wallet UNION select 'non-DSR')
        cross join unnest(sequence(date('2020-01-01'), current_date, interval '1' day)) as t(dt)
    ),
    grouped as (
        select
            dt,
            sum(case when wallet = 'DSR' then delta else 0 end) as dsr_delta,
            sum(case when wallet = 'non-DSR' then delta else 0 end) as non_dsr_delta
        from deltas2
        group by 1
    ),
    dsr as (
        select
            date(call_block_time) as dt,
            POWER((output_0/1e27), 365*24*60*60)-1 as dsr_rate
        from (
            select
                call_block_time,
                output_0,
                lag(output_0) over (order by call_block_time) as lag_output_0
            from maker_ethereum.pot_call_dsr
            where call_success
        )
        where output_0 <> coalesce(lag_output_0, cast(0 as UINT256))
    ),
    grouped_w_dsr as (
        select
            *,
            sum(case when dsr_rate IS not  NULL then 1 else 0 end) over (order by dt) as dsr_rate_grp
        from grouped
        left join dsr using (dt)
    ),
    balances as (
    select
        dt,
        sum(dsr_delta) over (order by dt) / 1e18 as dsr_balance,
        sum(non_dsr_delta) over (order by dt) / 1e18 as non_dsr_balance,
        sum(dsr_delta+non_dsr_delta) over (order by dt) / 1e18 as total_balance,
        max(dsr_rate) over (partition by dsr_rate_grp) as dsr_rate
        from grouped_w_dsr
    )

select * from balances
where dt > current_date - interval '13' month
order by dt desc nulls first