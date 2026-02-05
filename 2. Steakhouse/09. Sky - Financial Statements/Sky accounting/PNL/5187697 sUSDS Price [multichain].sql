/*
-- @title: sUSDS Overview multichain
-- @author: Steakhouse Financial
-- @description: Provides an overview of sUSDS on different blockchains, showing circulating supply, net flows, # of holders and USD value
-- @notes: deployment registry at https://github.com/sparkdotfi/spark-address-registry/blob/master/src/
-- @version:
    - 1.0 - 2025-01-23 - Initial version
    - 2.0 - 2025-02-27 - Multichain design
*/
with
    -- addresses to be excluded in sUDSD transfers
    excluded_addr(chain, addr, description) as (
        values
            ('base', 0x0000000000000000000000000000000000000000, 'burn address'),
            ('base', 0x2917956eff0b5eaf030abdb4ef4296df775009ca, 'preminted sUSDS in ALM Proxy'),
            ('base', 0x1601843c5e9bc251a3272907010afa41fa18347e, 'Spark PSM'),
            ('arbitrum', 0x0000000000000000000000000000000000000000, 'burn address'),
            ('arbitrum', 0x92afd6F2385a90e44da3a8B60fe36f6cBe1D8709, 'preminted sUSDS in ALM Proxy'),
            ('arbitrum', 0x2B05F8e1cACC6974fD79A673a341Fe1f58d27266, 'Spark PSM')  
    ),
    -- retrieve parameter changes in SSR Oracle
    ssr_oracle as (
        select
            date(evt_block_time) as dt,
            1 as ray,
            max_by(cast(json_extract_scalar(json_parse(nextData), '$.ssr') as uint256) / 1e27, evt_block_time) as ssr,
            max_by(cast(json_extract_scalar(json_parse(nextData), '$.chi') as uint256) / 1e27, evt_block_time) as chi,
            max_by(cast(json_extract_scalar(json_parse(nextData), '$.rho') as uint256), evt_block_time) as rho
        from sky_base.ssrauthoracle_evt_setsusdsdata
        group by 1, 2
    ),
    ssr_oracle_old as (
        select
            date(block_time) as dt,
            1 as ray,
            max_by(varbinary_to_uint256(varbinary_substring(data, 3, 30)), block_time) / 1e27 as ssr,
            max_by(varbinary_to_uint256(varbinary_substring(data, 35, 30)), block_time) / 1e27 as chi,
            max_by(varbinary_to_uint256(varbinary_substring(data, 67, 30)), block_time) as rho
        from base.logs
        where contract_address = 0x65d946e533748A998B1f0E430803e39A6388f7a1 -- SSR Oracle
          and topic0 = 0xc234856e2a0c5b406365714ced016892e7d98f7b1d49982cdd8db416a586d811 -- SetSUSDSData()
          and block_time > date '2024-11-01'
        group by 1
    ),
    -- create daily sequence and backfill oracle params
    params_seq as (
        select
            dt,
            to_unixtime(dt) as ts,
            1 as ray,
            coalesce(ssr, last_value(ssr) ignore nulls over (order by dt rows between unbounded preceding and current row)) as ssr,
            coalesce(chi, last_value(chi) ignore nulls over (order by dt rows between unbounded preceding and current row)) as chi,
            coalesce(rho, last_value(rho) ignore nulls over (order by dt rows between unbounded preceding and current row)) as rho
        from (
            select *
            from unnest(sequence(date '2024-11-04', current_date, interval '1' day)) as t(dt)
            left join ssr_oracle using(dt)
        )
    ),
    -- precalcs for the pricing
    base as (
        select
            dt,
            ray,
            chi,
            (ssr - ray) as rate,
            (ts - rho) as exp
        from params_seq
    ),
    -- calculate pricing based on binomial approximation, as described in:
    -- https://basescan.org/address/0x65d946e533748A998B1f0E430803e39A6388f7a1#code#F3#L66
    pricing as (
        select
            dt,
            chi * (ray + rate * exp + secondTerm + thirdTerm) / ray as price_usd
        from (
            select
                dt,
                ray as ray,
                chi as chi,
                exp,
                rate,
                exp * (exp - 1) * (power(rate, 2) / ray) / 2 as secondTerm,
                exp * (exp - 1) * (exp - 2) * (power(rate, 3) / power(ray,2)) / ray as thirdTerm
            from base
        )
    )

select * from pricing order by dt desc
