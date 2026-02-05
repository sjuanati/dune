/*
-- @title: Profit & Loss
-- @author: Steakhouse Financial
-- @description: Monthly income statement since 2020
-- @notes: N/A
-- @version:
    - 1.0 - 2024-06-05 - Initial version
    - 2.0 - 2024-07-11 - Liq. Expenses removed from Liq. Incomes, as they are already considered in Direct Expenses
                       - Added Liquidation Expenses detail (fd_id=1410) under Direct Costs
    - 3.0 - 2025-09-02 - Add Endgame Expenses
                        -> Add Returned Workforce Expenses into the Workforce Expenses total
    - 4.0 - 2025-11-25 - Add Star Revenues into Gross Interest Revenues related account ids
*/

with
    financial_items as (
        select fi_id, label, label_tab from query_3685347 where scope = 'pnl' -- PNL Items
    ),
    chart_of_accounts as (
        select cast(account_id as varchar) as account_id from query_3689733 -- Chart of Accounts v2
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
            label,
            label_tab,
            fi_id,
            sum(
                case fi_id
                    -- Total Non-Stability Fee Revenues
                    when 1050 then case
                        when account_id like '312%'  -- Liquidation Revenues
                          or account_id like '313%'  -- Trading Revenues
                          or account_id like '3119%' -- Stablecoins
                        then coalesce(sum_value, 0)
                    end
                    -- Trading Revenues
                    when 1100 then case
                        when account_id like '313%'  -- Trading Revenues
                          or account_id like '3119%' -- Stablecoins
                        then coalesce(sum_value, 0)
                    end
                    -- Liquidations Revenues
                    when 1200 then case
                        when account_id like '312%' -- Liquidation Revenues
                        then coalesce(sum_value, 0)
                    end
                    -- Σ Stability Fee Revenues (or Gross interest revenues)
                    when 1250 then case
                        when account_id like '311%' -- Gross interest revenues
                        then coalesce(sum_value, 0)
                    end
                    -- ETH
                    when 1300 then case
                        when account_id like '3111%' -- ETH
                        then coalesce(sum_value, 0)
                    end
                    -- BTC
                    when 1310 then case
                        when account_id like '3112%' -- BTC
                        then coalesce(sum_value, 0)
                    end
                    -- WSTETH
                    when 1305 then case
                        when account_id like '3113%' -- WSTETH
                        then coalesce(sum_value, 0)
                    end
                    -- Liquidity Pool
                    when 1315 then case
                        when account_id like '3114%' -- Liquidity Pool
                        then coalesce(sum_value, 0)
                    end
                    -- Money Market
                    when 1320 then case
                        when account_id like '3116%' -- Money Market
                        then coalesce(sum_value, 0)
                    end
                    -- -- Star Revenues
                    -- when 1326 then case
                    --     when account_id like '341%' -- Star Revenues
                    --     then coalesce(sum_value, 0)
                    -- end
                    -- PSM
                    when 1325 then case
                        when account_id like '3118%' -- PSM
                        then coalesce(sum_value, 0)
                    end
                    -- RWA
                    when 1330 then case
                        when account_id like '3117%' -- RWA
                        then coalesce(sum_value, 0)
                    end
                    -- Other
                    when 1335 then case
                        when account_id like '3115%' -- Other
                        then coalesce(sum_value, 0)
                    end
                    -- Stablecoins
                    when 1340 then case
                        when account_id like '3119%' -- Stablecoins
                        then coalesce(sum_value, 0)
                    end
                    -- Gross interest revenues
                    when 1350 then case
                        when account_id like '311%' -- Gross Interest Revenues
                          or account_id like '312%' -- Liquidation Revenues
                          or account_id like '313%' -- Trading Revenues
                        then coalesce(sum_value, 0)
                    end
                    -- Direct Expenses
                    when 1375 then case
                        when account_id like '316%' -- Direct Expenses
                        then coalesce(sum_value, 0)
                    end
                    -- DSR
                    when 1400 then case
                        when account_id like '3161%' -- DSR
                        then coalesce(sum_value, 0)
                    end
                    -- Oracle Gas Expenses
                    when 1405 then case
                        when account_id like '3163%' -- Oracle Gas Expenses
                        then coalesce(sum_value, 0)
                    end
                    -- Liquidation Expenses
                    when 1410 then case
                        when account_id like '3162%' -- Liquidation Expenses
                        then coalesce(sum_value, 0)
                    end
                	-- Sky Staking Expenses
                	when 1420 then case
                		when account_id like '3164%' -- Sky Staking Expenses
                		then coalesce(sum_value, 0)
                	end
                    -- Stability Fee Income
                    when 1500 then case
                        when account_id like '311%' -- Gross Interest Revenues
                          or account_id like '316%' -- Direct Expenses
                        then coalesce(sum_value, 0)
                    end
                    -- Total Net Revenues
                    when 1510 then case
                        when account_id like '311%' -- Gross Interest Revenues
                          or account_id like '312%' -- Liquidation Revenues
                          or account_id like '313%' -- Trading Revenues
                          or account_id like '316%' -- Direct Expenses
                        then coalesce(sum_value, 0)
                    end
                    -- Keeper Maintenance
                    when 1615 then case
                        when account_id like '3171%' -- Keeper Maintenance
                        then coalesce(sum_value, 0)
                    end
                    -- Workforce Expenses
                    when 1620 then case
                        when account_id like '31720' -- Workforce Expenses
                        or account_id like '31730' -- Returned Workforce Expenses
                        then coalesce(sum_value, 0)
                    end
                    -- Endgame Expenses
                    when 1621 then case
                        when account_id like '31721%' -- Endgame Expenses
                        then coalesce(sum_value, 0)
                    end
                    -- Direct to Third Party Expenses
                    when 1610 then case
                        when account_id like '3174%' -- Direct to Third Party Expenses
                        then coalesce(sum_value, 0)
                    end
                    -- MKR/SKY Token Expenses 
                    when 1625 then case
                          when account_id like '321%' -- MKR/SKY Token Expenses 
                        then coalesce(sum_value, 0)
                    end
                    -- Treasury Assets 
                    when 1630 then case
                          when account_id like '331%' -- Treasury Assets
                        then coalesce(sum_value, 0)
                    end
                    -- Total Operating Expenses
                    when 1650 then case
                        when account_id like '317%' -- Indirect Expenses
                          or account_id like '331%' -- DS Pause Proxy
                          or account_id like '321%' -- MKR/SKY Token Expenses
                        then coalesce(sum_value, 0)
                    end
                    -- Net Operating Earnings
                    when 1700 then case
                        when account_id like '311%' -- Gross Interest Revenues
                          or account_id like '312%' -- Liquidation Revenues
                          or account_id like '313%' -- Trading Revenues
                          or account_id like '316%' -- Direct Expenses
                          or account_id like '317%' -- Indirect Expenses
                          or account_id like '331%' -- DS Pause Proxy
                          or account_id like '321%' -- MKR/SKY Token Expenses
                        then coalesce(sum_value, 0)
                    end
                end
            ) as value,
            sum(
                case
                    when fi_id = 1000 then case      -- PnL
                        when account_id like '312%'  -- Liquidation Revenues
                        then sum_value
                    end
                end
            ) as liquidation_income,
            sum(
                case
                    when fi_id = 1000 then case      -- PnL
                        when account_id like '313%'  -- Trading Revenues
                          or account_id like '3119%' -- Stablecoins
                        then sum_value
                    end
                end
            ) as trading_income,
            sum(
                case when fi_id = 1000 then case -- PnL
                    when account_id like '311%'  -- Gross Interest Revenues
                    then sum_value
                    end
                end
            ) as lending_income,
            sum(
                case when fi_id = 1000 then case -- PnL
                    when account_id like '316%'  -- Direct Expenses
                    then sum_value
                    end
                end
            ) as direct_expenses,
            sum(
                case when fi_id = 1000 then case -- PnL
                    when account_id like '317%'  -- Indirect Expenses
                      or account_id like '331%'  -- DS Pause Proxy
                      or account_id like '321%'  -- MKR Token Expenses
                    then sum_value
                    end
                end
            ) as operating_expenses
        from periods
        cross join financial_items
        left join accounting using (year, month)
        group by 1, 2, 3, 4, 5, 6
    )
    
select
    year,
    month,
    period,
    fi_id,
    label,
    label_tab,
    value,
    if(liquidation_income < 1e-4, 0, liquidation_income) as "Liquidation Income",
    if(trading_income < 1e-4, 0, trading_income) as "Trading Fees",
    if(lending_income < 1e-4, 0, lending_income) as "Interest Income",
    direct_expenses as "Direct Expenses",
    operating_expenses as "Operating Expenses"
from pnl
where year > 2019
order by period desc, fi_id nulls first