/*
-- @title: Balance Sheet
-- @author: Steakhouse Financial
-- @description: Monthly balance sheet
-- @notes: N/A
-- @version:
    - 1.0 - 2023-12-07 - Initial version
    - 2.0 - 2024-10-28 - Account ID was not updated to 33110 from 32810.
    - 3.0 - 2024-11-03 - Update proprietary treasury code 32810 to 33110 in surplus buffer.
*/

with
    financial_items as (
        select fi_id, label_tab from query_3685347 where scope = 'bs' -- Financial Items
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
                    -- Crypto Vaults
                    when 2050 then case
                        when account_id like '11%' -- Collateralized Lending
                        then coalesce(cum_value, 0)
                        else 0
                    end
                    -- RWA Vaults
                    when 2150 then case
                        when account_id like '12%' -- Real-World Lending
                        --or account_id = '13411' -- TODO: ************* THIS IS  WRONG AND SHOULD BE FIXED *******************
                        then coalesce(cum_value, 0)
                        else 0
                    end
                    -- PSM Vaults
                    when 2100 then case
                        when account_id like '13%' -- Liquidity Pool
                        then coalesce(cum_value, 0)
                        else 0
                    end
                    -- Total CDP
                    when 2200 then case
                        when account_id like '11%' -- Collateralized Lending
                          or account_id like '12%' -- Real-World Lending
                          or account_id like '13%' -- Liquidity Pool
                          then coalesce(cum_value, 0)
                          else 0
                    end
                    -- Treasury Holdings
                    when 2250 then case
                        when account_id = '33110' -- a CoA tinc 331100 però a taula steakhouse tinc 32810!!!
                          or account_id like '39%'
                        then coalesce(cum_value, 0)
                    end
                    -- Star Vaults
                    when 2260 then case
                        when account_id like '15%'
                        then coalesce(cum_value, 0)
                    end
                    -- Spark Vaults
                    when 2261 then case
                        when account_id like '1501%'
                        then coalesce(cum_value, 0)
                    end
                    -- Grove Vaults
                    when 2262 then case
                        when account_id like '1502%'
                        then coalesce(cum_value, 0)
                    end
                    -- Obex Vaults
                    when 2263 then case
                        when account_id like '1503%'
                        then coalesce(cum_value, 0)
                    end
                    -- Backstop Capital
                    when 2270 then case
                        when account_id like '16%'
                        then coalesce(cum_value, 0)
                    end
                    -- Total Assets
                    when 2300 then case
                        when account_id like '1%' -- Assets
                        then coalesce(cum_value, 0)
                        else 0
                    end
                    -- 	Interest bearing Dai (DSR)
                    when 2400 then case
                        when account_id like '2111%' -- Interest-bearing
                        then coalesce(cum_value, 0)
                        else 0
                    end
                    -- 	Non-interest bearing Dai (Circulating)
                    when 2450 then case
                        when account_id like '2112%' -- Non-interest bearing
                        then coalesce(cum_value, 0)
                        else 0
                    end
                    -- Total Liabilities
                    when 2500 then case
                        when account_id like '2%' -- Liabilities
                        then coalesce(cum_value, 0)
                        else 0
                    end
                    -- Surplus buffer
                    when 2600 then case
                        -- TODO/TO BE UPDATED !!!!
                        when account_id like '3%'
                         and account_id not like '33%' -- Proprietary Treasury
                         and account_id not like '39%' -- Currency Translation to Presentation Token
                        then coalesce(cum_value, 0)
                        else 0
                    end
                    -- Treasury holdings
                    when 2650 then case
                        when account_id = '33110' -- a CoA tinc 331100 però a taula steakhouse tinc 32810!!!
                          or account_id like '39%'
                        then coalesce(cum_value, 0)
                        else 0
                    end
                    -- Total Equity
                    when 2700 then case
                        when account_id like '3%'
                        then coalesce(cum_value, 0)
                        else 0
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
    case when abs(value) <= 1e-6 and fi_id in (2250, 2650) then 0 else value end as value
from pnl
where year > 2019
order by period desc, fi_id asc