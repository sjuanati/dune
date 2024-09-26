/*
-- @title: Badger eBTC - Financial Items
-- @author: Steakhouse Financial
-- @description: Defines the accounting hierarchy, display order, and label formatting to show the PNL and BS
-- @notes: N/A
-- @version:
    - 1.0 - 2024-09-12 - Initial version
*/

with
    items as (
        ------ Statement of Earnings (PnL) ---------
        select 1000 as fi_id, 'pnl' as scope, 'PnL' as label, 'Profit & Loss' as label_tab
        union all
        select 1005, 'pnl', 'Revenues', '&nbsp;&nbsp;Revenues'
        union all
        select 1050, 'pnl', 'Fees', '&nbsp;&nbsp;&nbsp;&nbsp;⌄ Fees'
        union all
        select 1100, 'pnl', 'Protocol Yield Share', '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Protocol Yield Share'
        union all
        select 1125, 'pnl', 'Flash Loans', '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Flash Loans'
        union all
        select 1150, 'pnl', 'Redemptions', '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Redemptions'
        union all
        select 1370, 'pnl', 'Expenses', '&nbsp;&nbsp;Expenses'
        union all
        select 1375, 'pnl', 'Direct Expenses', '&nbsp;&nbsp;&nbsp;&nbsp;⌄ Direct Expenses'
        union all
        select 1400, 'pnl', 'Incentives', '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Incentives'
        union all
        select 1500, 'pnl', 'Net Earnings', '&nbsp;&nbsp;<b>Net Earnings</b>'
        union all
        ------ Balance Sheet (BS) ---------
        select 2000 as fi_id, 'bs' as scope, 'Balance Sheet' as label, 'Balance Sheet' as label_tab
        union all
        select 2005, 'bs', 'Liquidity - eBTC', '&nbsp;&nbsp;Liquidity - eBTC'
        union all
        select 2010, 'bs', 'Liquidity - stETH', '&nbsp;&nbsp;Liquidity - stETH'
        union all
        select 2015, 'bs', 'CDPs', '&nbsp;&nbsp;CDPs'
        union all
        select 2300, 'bs', 'Total Assets', '&nbsp;&nbsp;&nbsp;&nbsp;<b>Total Assets</b>'
        union all
        select 2350, 'bs', 'Liabilities', '&nbsp;&nbsp;Liabilities'
        union all
        select 2400, 'bs', 'eBTC', '&nbsp;&nbsp;&nbsp;&nbsp;eBTC'
        union all
        select 2500, 'bs', 'Total Liabilities', '&nbsp;&nbsp;&nbsp;&nbsp;<b>Total Liabilities</b>'
        union all
        select 2550, 'bs', 'Equity', '&nbsp;&nbsp;Equity'
        union all
        select 2600, 'bs', 'Financial Results', '&nbsp;&nbsp;&nbsp;&nbsp;Financial Results'
        union all
        select 2700, 'bs', 'Total Equity', '&nbsp;&nbsp;&nbsp;&nbsp;<b>Total Equity</b>'
    )

select * from items order by 1