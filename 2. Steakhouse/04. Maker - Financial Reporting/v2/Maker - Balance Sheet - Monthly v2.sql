/*
-- @title: Maker - Balance Sheet - Monthly
-- @author: Steakhouse Financial
-- @description: Calculates a simplified balance sheet on a monthly basis, grouping by assets,
                 liabilities, and capital, since year 2000.
-- @notes: N/A
-- @version:
    - 1.0 - 2023-09-21 - Initial version
    - 2.0 - 2024-02-19 - Refactored version in a monthly basis
*/

with
    bs as (
        select * from query_3704439 -- maker balance sheet
    ),
    pivot as (
        select month, 'Crypto Vaults' as item, total_crypto as value from bs
        union all
        select month, 'PSM Vaults', psm from bs
        union all
        select month, 'RWA Vaults', total_rwa from bs
        union all
        select month, 'Treasury Holdings', treasury from bs
        union all
        select month, 'DSR', -dsr from bs
        union all
        select month, 'DAI', -dai from bs
        union all
        select month, 'Equity', -total_equity from bs
    )

select * from pivot where cast(substr(month, 1, 4) as integer) > 2019