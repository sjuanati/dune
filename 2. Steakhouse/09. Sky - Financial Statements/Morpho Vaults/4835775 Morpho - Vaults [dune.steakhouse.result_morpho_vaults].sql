/*
@title: Morpho V2 - Vaults
@description: shows all Morpho vaults created, incl. the loan asset & decimals and some fancy formatting
@author: Steakhouse Financial
@dev: n/a
@version:
    - 1.0 - 2025-03-11 - Initial version
    - 2.0 - 2025-04-06 - Add version type
    - 3.0 - 2025-05-08 - Added multiple chain and move to get_hrefs 
    - 4.0 - 2025-07-16 - Add in Unichain and Katana links for vaults
    - 5.0 - 2025-08-05 - Add Vault Curator updated
    - 6.0 - 2025-08-24 - Add curator owner address
    - 7.0 - 2025-09-09 - Renamed 3 steakhouse vaults in Base
    - 8.0 - 2025-12-22 - Support owner_address showing the latest ownership transfer
*/

with
    vault_curator_change as (
        SELECT 
            chain as blockchain, contract_address, max_by(newCurator, evt_block_time) as curator
        FROM metamorpho_vaults_multichain.metamorphov1_1_evt_setcurator
        GROUP BY 1, 2
    )
    , vault_owner_change as (
        SELECT 
            chain as blockchain, contract_address, max_by(newOwner, evt_block_number * 1000 + evt_tx_index) as newOwner
        FROM metamorpho_vaults_multichain.metamorphov1_1_evt_ownershiptransferred
        GROUP BY 1, 2
    )
    , vault_creation as (
        select
            chain as blockchain,
            evt_tx_hash as tx_hash,
            evt_block_time as ts,
            metamorpho as contract_address,
            asset as token_address,
            caller,
            COALESCE(voc.newOwner, c.initialOwner) as owner_address,
            COALESCE(vcc.curator, c.initialOwner) as curator_address,
            name as vault_name,
            symbol as vault_symbol,
            version
        from (
            select *, 1.0 as version from metamorpho_factory_multichain.metamorphofactory_evt_createmetamorpho
            union all
            select *, 1.1 as version from metamorpho_factory_multichain.metamorphov1_1factory_evt_createmetamorpho
        ) as c
        LEFT JOIN vault_curator_change as vcc on c.chain = vcc.blockchain and c.contract_address = vcc.contract_address
        LEFT JOIN vault_owner_change as voc on c.chain = voc.blockchain and c.contract_address = voc.contract_address
    ),
    vault_name_change as (
        select chain, contract_address, max_by(name, evt_block_number) as vault_name
        from metamorpho_vaults_multichain.metamorphov1_1_evt_setname
        where name != ''
        group by 1, 2
    ),
    vault_symbol_change as (
        select chain, contract_address, max_by(symbol, evt_block_number) as vault_symbol
        from metamorpho_vaults_multichain.metamorphov1_1_evt_setsymbol
        where symbol != ''
        group by 1, 2
    ),
    
    vault_extended as (
        select distinct
            v.blockchain,
            v.tx_hash,
            v.ts,
            v.contract_address as vault_address,
            c.curator_name,
            v.caller,
            v.owner_address,
            v.curator_address,
            v.token_address,
            case
                when v.blockchain = 'base' and v.contract_address = 0xbeeF010f9cb27031ad51e3333f9aF9C6B1228183 then 'Steakhouse USDC Coinbase'
                when v.blockchain = 'base' and v.contract_address = 0xcbeef01994e24a60f7dcb8de98e75ad8bd4ad60d then 'Smokehouse USDC Coinbase'
                when v.blockchain = 'base' and v.contract_address = 0xbeefa74640a5f7c28966cba82466eed5609444e0 then 'Smokehouse USDC Old'
                else coalesce(n.vault_name, v.vault_name)
            end as vault_name,
            case
                when v.blockchain = 'base' and v.contract_address = 0xbeeF010f9cb27031ad51e3333f9aF9C6B1228183 then 'steakUSDC-cb'
                when v.blockchain = 'base' and v.contract_address = 0xcbeef01994e24a60f7dcb8de98e75ad8bd4ad60d then 'bbqUSDC-cb'
                when v.blockchain = 'base' and v.contract_address = 0xbeefa74640a5f7c28966cba82466eed5609444e0 then 'bbqUSDC-old'
                else coalesce(s.vault_symbol, v.vault_symbol)
            end as vault_symbol,
            t.symbol as token_symbol,
            t.decimals as token_decimals,
            version
        from vault_creation v
        left join dune.steakhouse.result_token_info t
            on v.token_address = t.token_address
            and v.blockchain = t.blockchain
        left join dune.steakhouse.dataset_morpho_vault_curators c
            on v.curator_address = c.owner_address
            and v.blockchain = c.blockchain
        left join vault_name_change n
            on v.blockchain = n.chain
            and v.contract_address = n.contract_address
        left join vault_symbol_change s
            on v.blockchain = s.chain
            and v.contract_address = s.contract_address
    ),
    vault_formatted as (
        select
            blockchain,
            caller,
            concat(
                format('%1$TY-%1$Tm-%1$Td %1$TH:%1$TM', ts),
                ' ',
                CASE WHEN blockchain = 'katana'
                    THEN get_href(format('https://ww4.katanascan.com/tx/%s', tx_hash), '🔗')
                    ELSE get_href(get_chain_explorer_tx_hash(blockchain, tx_hash), '🔗')
                END
            ) as creation_ts,
            curator_name,
            owner_address,
            curator_address,
            concat(
                if(vault_symbol != '', vault_symbol, 'N/A'), ' ',
                get_href(
                    CASE 
                        WHEN vault_symbol = '' THEN format('https://legacy.morpho.org/vault?vault=%s&network=%s', vault_address, IF(blockchain = 'ethereum', 'mainnet', blockchain))
                        WHEN blockchain in ('ethereum', 'base', 'katana', 'unichain') THEN format('https://app.morpho.org/%s/vault/%s/', blockchain, vault_address)
                        WHEN blockchain = 'polygon' THEN format('https://www.compound.blue/%s', vault_address)
                        WHEN blockchain in ('worldchain', 'corn') THEN format('https://oku.trade/morpho/vaults?inputChain=%s&selectedVault=%s', blockchain, vault_address)
                        WHEN blockchain = 'optimism' THEN FORMAT('https://moonwell.fi/vaults/deposit/%s/%s', blockchain, vault_symbol)
                        -- Not launched on Dune
                        WHEN blockchain = 'hyperevm' THEN format('https://app.hyperbeat.org/vaults/%s', LOWER(vault_Symbol))
                        else format('https://legacy.morpho.org/vault?vault=%s&network=%s', vault_address, IF(blockchain = 'ethereum', 'mainnet', blockchain))                       
                    END
                , '🔗')
            ) as vault,
            vault_name,
            concat(
                if(token_symbol != '', token_symbol, 'N/A'), ' ',
                CASE WHEN blockchain = 'katana'
                    THEN get_href(format('https://ww4.katanascan.com/address/%s', token_address), '🔗')
                    ELSE get_href(get_chain_explorer_address(blockchain, token_address), '🔗')
                END
            ) as token,
            ts as creation_date,
            vault_symbol,
            vault_address,
            token_symbol,
            token_address,
            token_decimals,
            version
        from vault_extended
    )

select * from vault_formatted order by creation_ts desc