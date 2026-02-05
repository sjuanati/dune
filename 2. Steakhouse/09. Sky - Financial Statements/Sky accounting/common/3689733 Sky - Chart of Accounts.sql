/*
-- @title: Chart of Accounts
-- @author: Steakhouse Financial
-- @description: provides Sky's chart of accounts
-- @notes: source data from https://docs.google.com/spreadsheets/d/15831d6VrtodvmPv83fFO7H7uvhDhBsnJlCetvHbQdjE
-- @version:
    - 1.0 - 2024-05-02 - Initial version
    - 2.0 - 2024-10-29 - Using the new version containing USDs CoA.
    - 3.0 - 2024-11-06 - Update the CoA dataset source
*/

select
    account_id,
    primary_label,
    secondary_label,
    account_label,
    category_label,
    subcategory_label
from dune.steakhouse.dataset_sky_coa
order by 1