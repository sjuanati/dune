/*
-- @title: Sky Incentives Facilitators Wallets
-- @author: Steakhouse Financial
-- @description: provides the most relevant with faciliators wallets interacting with Sky
-- @version:
    - 1.0 - 2024-10-31 - Initial version
*/

WITH facilitator_wallets(wallet_address, wallet_label) as (
    VALUES
    (   
        -- https://vote.makerdao.com/executive/template-executive-vote-seal-engine-initialization-rwa-vault-debt-ceiling-housekeeping-pinwheel-dao-resolution-aave-sparklend-revenue-share-payment-for-q3-2024-spark-proxy-spell-october-17-2024
        0x14d98650d46bf7679bbd05d4f615a1547c87bf68, 'Accessibility Facilitators'
        
    ), 
    (
        -- https://developers.sky.money/deployments/deployment-tracker
        0x2f0c88e935db5a60dda73b0b4eaeef55883896d9, 'SKY Vested Rewards Distribution'
    
    )


)

SELECT wallet_address
    , wallet_label
FROM facilitator_wallets