/*
-- @title: SKY - Protocol Capital
-- @author: Steakhouse Financial
-- @source: https://docs.google.com/document/d/1dtOgz_8WMuvQ608Q4yKVaOtzGZyfkNwAjGDcL0pMJxw/edit?tab=t.0
-- @version:
    - 1.0 - 2024-05-20 - Initial version
    - 2.0 - 2025-05-31 - Assign correct accounting ids for the final pnl calcs.
                        -> Move usage of 32810 (from prior codes) -> '33%' (proprietary treasury)
                        -> Add Sin flows
                        -> Move usage of specific codes for token expenses to '321%' to incorporate SKY rewards
*/

with
    financial_items as (
        select fi_id, label_tab from query_3685347 where scope = 'pc' -- Financial Items
    ),
    chart_of_accounts as (
        select cast(account_id as varchar) as account_id from query_3689733 -- Chart of Accounts
    ),
    accounting as (
        select * from query_3739530 -- Accounting Aggregated
    ),
    periods as (
        select
            date(period) as period,
            extract(year from period) as year,
            extract(month from period) as month
        from (
            select period
              from unnest(sequence(date('2019-11-01'), date_trunc('month', current_date), interval '1' month)) as t(period)
        )
    ),
    pnl as (
        select
            extract(year from period) as year,
            extract(month from period) as month,
            date_format(period, '%Y-%m') as period,
            label_tab,
            fi_id,
            sum(
                case fi_id
                    -- Net Operating Earnings
                    when 3050 then case
                        when account_id like '311%' -- Gross Interest Revenues
                          or account_id like '312%' -- Liquidation Revenues
                          or account_id like '313%' -- Trading Revenues
                          or account_id like '316%' -- Direct Expenses
                          or account_id like '317%' -- Indirect Expenses
                          or account_id like '318%' -- SubDAO Allocation
                          or account_id like '33%' -- Proprietary Treasury
                          or account_id like '321%' -- MKR/SKY Token Expenses
                          or account_id like '322%' -- MKR/SKY Contra Equity
                        then coalesce(sum_value, 0)
                    end
                    -- Issuance for MKR token expenses
                    when 3100 then case
                        when account_id like '321%' -- MKR/SKY Token Expenses
                        then coalesce(-sum_value, 0)
                    end
                    -- MKR/SKY mints/(burns)
                    when 3150 then case
                        when account_id like '314%' -- MKR/SKY Mints Burns
                        then coalesce(sum_value, 0)
                    end
                    -- SKY Staking Expenses
                    when 3160 then case
                        when account_id like '31640' -- SKY Staking Expenses
                        then coalesce(sum_value, 0)
                    end
                    -- SubDAO Allocation
                    when 3170 then case
                        when account_id like '318%' -- SubDAO Allocation
                        then coalesce(sum_value, 0)
                    end
                    -- Net Change in Surplus Buffer (Net Operating Earnings excl. MKR token expenses + MKR mints/burns)
                    when 3200 then case
                        when account_id like '311%' -- Gross Interest Revenues
                          or account_id like '312%' -- Liquidation Revenues
                          or account_id like '313%' -- Trading Revenues
                          or account_id like '314%' -- MKR Mints Burns
                          or account_id like '315%' -- Sin Flows
                          or account_id like '316%' -- Direct Expenses
                          or account_id like '317%' -- Indirect Expenses
                          or account_id like '318%' -- SubDAO Allocation
                        then coalesce(sum_value, 0)
                    end
                    -- Treasury asset income
                    when 3250 then case
                        when account_id like '33%' -- Proprietary Treasury
                        then coalesce(sum_value, 0)
                    end
                    -- Treasury asset chg value
                    when 3300 then case
                        when account_id like '39%' -- Currency Translation to Presentation Token
                        then coalesce(sum_value, 0)
                    end
                    -- Other changes in protocol capital
                    when 3350 then case
                        when account_id like '33%' -- Proprietary Treasury
                          or account_id like '39%' -- Currency Translation to Presentation Token
                        then coalesce(sum_value, 0)
                    end
                    -- Net Change in Protocol Capital (Net Change in Surplus Buffer + Other changes in Protocol Capital)
                    when 3400 then case
                        when account_id like '311%' -- Gross Interest Revenues
                          or account_id like '312%' -- Liquidation Revenues
                          or account_id like '313%' -- Trading Revenues
                          or account_id like '314%' -- MKR/SKY Mints Burns
                          or account_id like '315%' -- Sin Flows
                          or account_id like '316%' -- Direct Expenses
                          or account_id like '317%' -- Indirect Expenses
                          or account_id like '318%' -- SubDAO Allocation
                          or account_id like '33%' -- Proprietary Treasury
                          or account_id like '39%' -- Currency Translation to Presentation Token
                        then coalesce(sum_value, 0)
                    end
                end
            ) as value
        from periods
        cross join financial_items
        left join accounting using (year, month)
        group by 1, 2, 3, 4, 5
    )

select
    year,
    month,
    period,
    fi_id,
    label_tab,
    value
from pnl
where year > 2019
order by period desc, fi_id asc
