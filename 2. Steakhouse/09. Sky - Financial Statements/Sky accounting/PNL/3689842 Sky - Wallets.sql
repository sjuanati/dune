/*
-- @title: Wallets
-- @author: Steakhouse Financial
-- @description: provides the most relevant wallets to operations interacting with Sky
-- @notes: dataset -> https://docs.google.com/spreadsheets/d/1UYOGmIVZmTb273jov7lHXT9T82oopV_dNlXUEadfdK0
-- @version:
    - 1.0 - 2023-10-27 - Initial version
    - 2.0 - 2024-05-02 - Dataset version
    - 3.0 - 2024-10-31 - Change header
*/

select
    wallet_address,
    wallet_label,
    varfix,
    code
from dune.steakhouse.dataset_sky_wallets
order by code