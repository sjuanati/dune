/*
-- @title: Collaterals - Detailed
-- @author: Steakhouse Financial
-- @description: retrieves all collaterals (ilks) from the parameters table + source tables 
-- @notes:
--      - source data from https://docs.google.com/spreadsheets/d/1giHwSFYSlZJQGhhejiYWJwXhscQKTKCXdtsp6ZX7yHs
--      - some fields might have different values depending on the 'start' and 'end' 
-- @version:
    - 1.0 - 2024-04-30 - Initial version
    - 2.0 - 2024-10-29 - Merge query from collaterals detailed.
*/

with
  ilks as (
    select from_utf8(bytearray_rtrim(ilk)) as ilk
    from (
        select distinct(i) as ilk
        from maker_ethereum.vat_call_frob
        union
        select distinct(ilk)
        from maker_ethereum.spot_call_file
        union
        select distinct(ilk)
        from maker_ethereum.jug_call_file
    )
  ),
  metadata as (
    select
        i.ilk,
        p.name,
        p.alm_category,
        p.alm_sub_category,
        p.collateral_type,
        p.asset_account_id,
        p.equity_account_id,
        date(coalesce(p.start, '2015-01-01')) as start,
        date(coalesce(p."end", '2099-12-31')) as "end",
        cast(coalesce(expected_annual_return, 0.0) as double) as expected_annual_return,
        cast(coalesce(p.capital_at_risk, 0.0) as double) as capital_at_risk, 
        cast(coalesce(p.duration, 0.0) as double) as duration, 
        cast(coalesce(p.duration, 0.0) as double)*0.02 as duration_risk,
        cast(coalesce(p.maturity, 0.0) as double) as maturity, 
        cast(coalesce(p.credit_risk, 0.0) as double) as credit_risk, 
        cast(coalesce(p.crypto_market_risk, 0.0) as double) as crypto_market_risk, 
        cast(coalesce(p.operational_risk, 0.0) as double) as operational_risk, 
        cast(coalesce(p.liquidity_day, 0.0) as double) as liquidity_day, 
        cast(coalesce(p.liquidity_week, 0.0) as double) as liquidity_week, 
        cast(coalesce(p.liquidity_month, 0.0) as double) as liquidity_month, 
        cast(coalesce(p.liquidity_year, 0.0) as double) as liquidity_year
    from ilks i
    left join dune.steakhouse.dataset_sky_ilks_params p
        on i.ilk = p.ilk
    where i.ilk is not null
  )

select *
from metadata
order by 1 asc