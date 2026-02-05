/*
-- @title: Sky Incentive Contracts
-- @author: Steakhouse Financial
-- @description: provides the most relevant incentive contracts interacting with Sky
-- @version:
    - 1.0 - 2024-10-31 - Initial version
*/
WITH incentive_contracts(contract_address, contract_name) as (
    VALUES
    (0x0650caf159c5a49f711e8169d4336ecb9b950275, 'UsdsSkyRewards'),
    (0xca9ef7f3404b23c77a2a0dee8ab54b3338d35eae, 'Early Bird Rewards') -- MerkleDistributor
)

SELECT contract_address
    , contract_name
FROM incentive_contracts