/*
-- @title: PNL Items
-- @description: Master Data query for financial items on Profit & Loss, Balance Sheet & Protocol Capital
-- @author: Steakhouse Financial
-- @notes: N/A
-- @version:
    - 1.0 - 2024-06-19 - Initial version
    - 2.0 - 2025-07-23 - Add Sky Staking Expenses
    - 3.0 - 2025-09-03 - Add Endgame expenses
    - 4.0 - 2025-11-25 - Add Star Revenues
    - 5.0 - 2025-01-06 - Switch position of Other to after RWA
*/

with
    items as (
        ------ Statement of Earnings (PnL) ---------
        select 1000 as fi_id, 'pnl' as scope, 'PnL' as label, 'Profit & Loss' as label_tab
        union all
        select 1001, 'pnl', 'Revenues', '&nbsp;&nbsp;Revenues'
        union all
        select 1050, 'pnl', 'Non-Stability Fee Revenues', '&nbsp;&nbsp;&nbsp;&nbsp;⌄ Non-Stability Fee Revenues'
        union all
        select 1100, 'pnl', 'Trading Revenues', '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Trading Revenues'
        union all
        select 1200, 'pnl', 'Liquidations Revenues', '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Liquidations Revenues'
        union all
        select 1250, 'pnl', 'Stability Fee Revenues', '&nbsp;&nbsp;&nbsp;&nbsp;⌄ Stability Fee Revenues'
        union all
        select 1300, 'pnl', 'ETH', '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;ETH'
        union all
        select 1305, 'pnl', 'WSTETH', '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;WSTETH'
        union all
        select 1310, 'pnl', 'BTC', '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;BTC'
        union all
        select 1315, 'pnl', 'Liquidity Pool', '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Liquidity Pool'
        union all
        select 1320, 'pnl', 'Money Market', '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Money Market'
        union all
        -- union all
        -- select 1326, 'pnl', 'Star Revenues', '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Star Revenues'
        select 1325, 'pnl', 'PSM', '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;PSM'
        union all
        select 1330, 'pnl', 'RWA', '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;RWA'
        union all
        select 1335, 'pnl', 'Other', '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Other'
        union all
        select 1340, 'pnl', 'Stablecoins', '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Stablecoins'
        union all
        select 1350, 'pnl', 'Gross Interest Revenues', '&nbsp;&nbsp;&nbsp;&nbsp;<b>Gross Interest Revenues</b>'
        union all
        select 1375, 'pnl', 'Direct Expenses', '&nbsp;&nbsp;&nbsp;&nbsp;⌄ Direct Expenses'
        union all
        select 1400, 'pnl', 'DSR', '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;DSR'
        union all
        select 1405, 'pnl', 'Oracle Gas Expenses', '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Oracle Gas Expenses'
        union all
        select 1410, 'pnl', 'Liquidation Expenses', '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Liquidation Expenses'
        -- union all
        -- select 1420, 'pnl', 'Sky Staking Expenses', '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Sky Staking Expenses'
        union all
        select 1500, 'pnl', 'Net Stability Fee Income', '&nbsp;&nbsp;&nbsp;&nbsp;<b>Net Stability Fee Income</b>'
        union all
        select 1510, 'pnl', 'Net Revenues', '&nbsp;&nbsp;&nbsp;&nbsp;<b>Net Revenues</b>'
        union all
        select 1600, 'pnl', 'Operating expenses', '&nbsp;&nbsp;Operating expenses'
        union all
        select 1610, 'pnl', 'Direct to Third Party Expenses', '&nbsp;&nbsp;&nbsp;&nbsp;Direct to Third Party Expenses'
        union all
        select 1615, 'pnl', 'Keeper Maintenance', '&nbsp;&nbsp;&nbsp;&nbsp;Keeper Maintenance'
        union all
        select 1620, 'pnl', 'Workforce Expenses', '&nbsp;&nbsp;&nbsp;&nbsp;Workforce Expenses'
        union all
        select 1621, 'pnl', 'Endgame Expenses', '&nbsp;&nbsp;&nbsp;&nbsp;Endgame Expenses'
        union all
        select 1625, 'pnl', 'MKR Token Expenses', '&nbsp;&nbsp;&nbsp;&nbsp;MKR Token Expenses'
        union all
        select 1630, 'pnl', 'Treasury Assets', '&nbsp;&nbsp;&nbsp;&nbsp;Treasury Assets'
        union all
        select 1650, 'pnl', 'Operating Expenses', '&nbsp;&nbsp;&nbsp;&nbsp;<b>Operating Expenses</b>'
        union all
        select 1700, 'pnl', 'Net Operating Earnings', '&nbsp;&nbsp;<b>Net Operating Earnings</b>'
        union all
        ------ Balance Sheet ---------
        select 2000 as fi_id, 'bs' as scope, 'Balance Sheet' as label, 'Balance Sheet' as label_tab
        union all
        select 2001, 'bs', 'Assets', '&nbsp;&nbsp;Assets'
        union all
        select 2050, 'bs', 'Crypto Vaults', '&nbsp;&nbsp;&nbsp;&nbsp;Crypto Vaults'
        union all
        select 2100, 'bs', 'PSM Vaults', '&nbsp;&nbsp;&nbsp;&nbsp;PSM Vaults'
        union all
        select 2150, 'bs', 'RWA Vaults', '&nbsp;&nbsp;&nbsp;&nbsp;RWA Vaults'
        union all
        select 2200, 'bs', 'Total CDP', '&nbsp;&nbsp;&nbsp;&nbsp;<b>Total CDP</b>'
        union all
        select 2250, 'bs', 'Treasury Holdings', '&nbsp;&nbsp;&nbsp;&nbsp;Treasury Holdings'
        union all
        select 2260, 'bs', 'Star Vaults', '&nbsp;&nbsp;&nbsp;&nbsp;Star Vaults'
        union all
        select 2261, 'bs', 'Spark Vaults', '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Spark Vaults'
        union all
        select 2262, 'bs', 'Grove Vaults', '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Grove Vaults'
        union all
        select 2263, 'bs', 'Obex Vaults', '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Obex Vaults'
        union all
        select 2270, 'bs', 'Backstop Capital', '&nbsp;&nbsp;&nbsp;&nbsp;Backstop Capital'
        union all
        select 2300, 'bs', 'Total Assets', '&nbsp;&nbsp;&nbsp;&nbsp;<b>Total Assets</b>'
        union all
        select 2350, 'bs', 'Liabilities', '&nbsp;&nbsp;Liabilities'
        union all
        select 2400, 'bs', 'Interest bearing Dai (DSR)', '&nbsp;&nbsp;&nbsp;&nbsp;Interest bearing Dai (DSR)'
        union all
        select 2450, 'bs', 'Non-interest bearing Dai (Circulating)', '&nbsp;&nbsp;&nbsp;&nbsp;Non-interest bearing Dai (Circulating)'
        union all
        select 2500, 'bs', 'Total Liabilities', '&nbsp;&nbsp;&nbsp;&nbsp;<b>Total Liabilities</b>'
        union all
        select 2550, 'bs', 'Equity', '&nbsp;&nbsp;Equity'
        union all
        select 2600, 'bs', 'Surplus buffer', '&nbsp;&nbsp;&nbsp;&nbsp;Surplus buffer'
        union all
        select 2650, 'bs', 'Treasury holdings', '&nbsp;&nbsp;&nbsp;&nbsp;Treasury holdings'
        union all
        select 2700, 'bs', 'Total Equity', '&nbsp;&nbsp;&nbsp;&nbsp;<b>Total Equity</b>'
        union all
        ------ Protocol Capital ---------
        select 3000 as fi_id, 'pc' as scope, 'Protocol Capital' as label, 'Protocol Capital' as label_tab
        union all
        select 3050, 'pc', 'Net Operating Earnings', '&nbsp;&nbsp;Net Operating Earnings'
        union all
        select 3100, 'pc', 'Issuance for MKR token expenses', '&nbsp;&nbsp;&nbsp;&nbsp;Issuance for MKR token expenses'
        union all
        select 3150, 'pc', 'MKR mints/(burns)', '&nbsp;&nbsp;&nbsp;&nbsp;MKR mints/(burns)'
        union all
        select 3160, 'pc', 'SKY Staking Expenses', '&nbsp;&nbsp;&nbsp;&nbsp;SKY Staking Expenses'
        union all
        select 3170, 'pc', 'SubDAO Allocation', '&nbsp;&nbsp;&nbsp;&nbsp;SubDAO Allocation'
        union all
        select 3200, 'pc', 'Net Change in Surplus Buffer', '&nbsp;&nbsp;Net Change in Surplus Buffer'
        union all
        select 3250, 'pc', 'Treasury asset income', '&nbsp;&nbsp;&nbsp;&nbsp;Treasury asset income'
        union all
        select 3300, 'pc', 'Treasury asset chg value', '&nbsp;&nbsp;&nbsp;&nbsp;Treasury asset chg value'
        union all
        select 3350, 'pc', 'Other changes in protocol capital', '&nbsp;&nbsp;Other changes in protocol capital'
        union all
        select 3400, 'pc', 'Net Change in Protocol Capital', '&nbsp;&nbsp;Net Change in Protocol Capital'
    )

select * from items order by 1