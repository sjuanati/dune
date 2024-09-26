/*
-- @title: Badger eBTC - CDPs at Risk
-- @author: Steakhouse Financial
-- @description: shows debt, collateral, various collateral ratios, and a risk indicator for open CDPs
-- @notes: collateral at risk is the deposited stETH without yielding or pys fee; if liquidated,
           the yield will be transferred to the liquidatooor
-- @version:
        1.0 - 2024-09-03 - Initial version
*/

with
    cdp_in_risk as (
        select
            *,
            case
                when icr < 1.25 then '🔴'
                when icr < 1.5 then '🟠'
                else '🟢'
            end as risk,
            case
                when icr < 1.25 then 'Death Zone'
                when icr < 1.5 then 'Danger Zone'
                else 'Healthy Zone'
            end as risk_desc
        from query_4040136 -- Badger - eBTC CDPs
        where dt = current_date
        and debt > 0
    ),
    -- @todo: IRC is calculated using non-yielding or yielding stETH?
    capital_in_risk as (
        select
            sum(coll_pys) as coll_total_risk,
            sum(coll_pys_usd) as coll_total_risk_usd
        from cdp_in_risk
        where icr < 1.25
    )

select
    risk,
    risk_desc,
    icr,
    mcr,
    ccr,
    cdp_number,
    concat(
        '<a href="https://debank.com/profile/',
        cast(borrower as varchar),
        '" target="_blank" >',
        "left"(cast(borrower as varchar), 6),
        '...',
        "right"(cast(borrower as varchar), 4),
        '</a>'
    ) as borrower_link,
    coll_pys,
    debt,
    coll_pys_usd,
    debt_usd,
    r.coll_total_risk,
    r.coll_total_risk_usd
from cdp_in_risk
cross join capital_in_risk r
order by icr asc