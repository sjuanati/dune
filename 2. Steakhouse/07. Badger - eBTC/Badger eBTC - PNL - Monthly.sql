/*
-- @title: Badger eBTC - PNL - Monthly
-- @author: Steakhouse Financial
-- @description: Monthly income statement
-- @notes: N/A
-- @version:
    - 1.0 - 2024-09-12 - Initial version
    - 2.0 - 2024-09-24 - Parametrized value to use either <value_usd> for US dollar value or <amount_base> for eBTC amount
*/

with
    financial_items as (
        select fi_id, label, label_tab from query_4065755 where scope = 'pnl' -- Financial Items
    ),
    accounting as (
        select * from dune.steakhouse.result_badger_ebtc_accounting -- Accounting
    ),
    periods as (
        select dt from unnest(sequence(date('2024-03-01'), date_trunc('month', current_date), interval '1' month)) as t(dt)
    ),
    pnl as (
        select
            p.dt,
            label,
            label_tab,
            fi_id,
            sum(
                case fi_id
                    -- Fees
                    when 1050 then case
                        when a.account_id like '30101%' -- Rev. Fees
                        then coalesce(a.{{value}}, 0)
                    end
                    -- Protocol Yield Share fees
                    when 1100 then case
                        when a.account_id = '3010101' -- Rev. Platform Fees
                         and a.metadata = 'PYS'
                        then coalesce(a.{{value}}, 0)
                    end
                    -- Flash Loan fees
                    when 1125 then case
                        when a.account_id = '3010101' -- Rev. Platform Fees
                         and a.metadata = 'FLASH-LOANS'
                        then coalesce(a.{{value}}, 0)
                    end
                    -- Redemption fees
                    when 1150 then case
                        when a.account_id = '3010101' -- Rev. Platform Fees
                         and a.metadata = 'REDEMPTIONS'
                        then coalesce(a.{{value}}, 0)
                    end
                    -- Revenues
                    when 1005 then case
                        when a.account_id = '3010101'  -- Rev. Platform Fees
                        then coalesce(a.{{value}}, 0)
                    end
                    -- Expenses
                    when 1370 then case
                        when a.account_id like '302%' -- Expenses
                        then coalesce(a.{{value}}, 0)
                    end
                    -- Direct Expenses
                    when 1375 then case
                        when a.account_id like '30201%' -- Direct Expenses
                        then coalesce(a.{{value}}, 0)
                    end
                    -- Incentives
                    when 1400 then case
                        when a.account_id = '3020104' -- Incentives
                        then coalesce(a.{{value}}, 0)
                    end
                    -- Net Earnings 
                    when 1500 then case
                        when a.account_id like '301%'
                          or a.account_id like '302%' 
                        then coalesce(a.{{value}}, 0)
                    end
                end
            ) as value,
            coalesce(
                sum(
                    case when fi_id = 1000 then case -- PnL
                        when a.account_id like '3010101'  -- Rev. Platform Fees
                         and a.metadata = 'FLASH-LOANS'
                        then a.{{value}}
                        end
                    end
                ), 0
            ) as rev_fl,
            coalesce(
                sum(
                    case when fi_id = 1000 then case -- PnL
                        when a.account_id like '3010101'  -- Rev. Platform Fees
                         and a.metadata = 'PYS'
                        then a.{{value}}
                        end
                    end
                ), 0
            ) as rev_pys,
            coalesce(
                sum(
                    case when fi_id = 1000 then case -- PnL
                        when a.account_id like '3010101'  -- Rev. Platform Fees
                         and a.metadata = 'REDEMPTIONS'
                        then a.{{value}}
                        end
                    end
                ), 0
            ) as rev_redempt,
            coalesce(
                sum(
                    case when fi_id = 1000 then case -- PnL
                        when a.account_id like '3020104'  -- Exp. Incentives
                        then a.{{value}}
                        end
                    end
                ), 0
            ) as exp_incentives
        from periods p
        cross join financial_items
        left join accounting a on p.dt = date_trunc('month', a.ts)
        group by 1, 2, 3, 4
    )

select *
from pnl
order by dt desc, fi_id nulls first

