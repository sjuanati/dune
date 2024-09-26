/*
-- @title: Badger eBTC - BS - Monthly
-- @author: Steakhouse Financial
-- @description: Monthly balance sheet
-- @notes: N/A
-- @version:
    - 1.0 - 2024-09-05 - Initial version
*/

with
    periods as (
        select dt from unnest(sequence(date('2024-03-01'), date_trunc('month', current_date), interval '1' month)) as t(dt)
    ),
    tokens as (
        select 0x661c70333aa1850ccdbae82776bb436a0fcfeefb as token_addr -- eBTC
        union all
        select 0xae7ab96520de3a18e5e111b5eaab095312d7fe84 as token_addr -- stETH
    ),
    coa as (
        select distinct(account_id) as account_id from dune.steakhouse.result_badger_ebtc_accounting
    ),
    monthly_base_price as (
        select
            dt,
            price_btc
        from query_4042988 -- Conversions
        where dt = last_day_of_month(dt) or dt = current_date
    ),
    bs_agg as (
        select
            dt,
            account_id,
            token_addr,
            sum(value_usd) as value_usd,
            sum(value_usd_m) as value_usd_m,
            sum(amount_base) as amount_base
        from (
            select
                date_trunc('month', a.ts) as dt,
                a.account_id,
                a.token_addr,
                a.value_usd,
                a.amount_base * p.price_btc as value_usd_m,
                a.amount_base
            from dune.steakhouse.result_badger_ebtc_accounting a -- Accounting
            left join monthly_base_price p on date_trunc('month', a.ts) = date_trunc('month', p.dt)
            union all
            select
                p.dt,
                c.account_id,
                t.token_addr,
                0 as value_usd,
                0 as value_usd_m,
                0 as amount_base
            from periods p
            cross join coa c
            cross join tokens t
        )
        group by 1, 2, 3
    ),
    bs_cum as (
        select
            dt,
            account_id,
            case
                when token_addr = 0x661c70333aa1850ccdbae82776bb436a0fcfeefb then 'EBTC'
                when token_addr = 0xae7ab96520de3a18e5e111b5eaab095312d7fe84 then 'STETH'
                else 'NEW_TOKEN'
            end as token,
            sum(value_usd) over (partition by account_id, token_addr order by dt asc) as value_usd,
            sum(value_usd_m) over (partition by account_id, token_addr order by dt asc) as value_usd_m,
            sum(amount_base) over (partition by account_id, token_addr order by dt asc) as amount_base
        from bs_agg
    ),
    -- @dev: don't add field <token> if only one token is used for that account; otherwise, it will show 'Unknown' items
    -- @dev: fi_id codes are defined in query_4065755
    bs_cats as (
        select
            dt,
            case
                when account_id like '101%' and token = 'EBTC' then 2005 -- Liquidity - eBTC
                when account_id like '101%' and token = 'STETH' then 2010 -- Liquidity - stETH
                when account_id like '102%' then 2015 -- CDPs
                when account_id like '201%' then 2400 -- eBTC
                when account_id like '30%' then 2600 -- Financial Results
                else 0 -- 'Unknown'
            end as fi_id,
            sum(if(account_id like '1%', value_usd, -value_usd)) as value_usd,
            sum(if(account_id like '1%', value_usd_m, -value_usd_m)) as value_usd_m,
            sum(if(account_id like '1%', amount_base, -amount_base)) as amount_base
        from bs_cum
        group by 1, 2
    ),
    bs_cats_desc as (
        select
            dt,
            fi_id,
            fi.label as item,
            value_usd,
            value_usd_m,
            amount_base
        from bs_cats c
        left join query_4065755 fi using (fi_id) -- Financial Items
    )

select * from bs_cats_desc order by dt asc