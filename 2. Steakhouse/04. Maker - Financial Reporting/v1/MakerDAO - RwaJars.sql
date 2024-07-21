with jars as (
    select 'Gemini' as label, 
        0x6934218d8B3E9ffCABEE8cd80F4c1C4167Afa638 as input, 
        0xf2e7a5b83525c3017383deed19bb05fe34a62c27 as jar, 
        0x056fd409e1d7a124bd7017459dfea2f387b6d5cd as currency, -- GUSD
        2 as decimals
    union all
    select 'MIP65' as label, 
        0xc8bb4e2B249703640e89265e2Ae7c9D5eA2aF742 as input, 
        0xef1B095F700BE471981aae025f92B03091c3AD47 as jar, 
        0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 as currency, -- USDC
        6 as decimals
    union all
    select 'HVB' as label, 
        0x6C6d4Be2223B5d202263515351034861dD9aFdb6 as input, 
        0x6C6d4Be2223B5d202263515351034861dD9aFdb6 as jar, 
        0x6b175474e89094c44da98b954eedeac495271d0f as currency, -- DAI
        18 as decimals
    union all
    select 'Coinbase' as label, 
        0x391470cD3D8307AdC051d878A95Fa9459F800Dbc as input, 
        0x71eC6d5Ee95B12062139311CA1fE8FD698Cbe0Cf as jar, 
        0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 as currency, -- USDC
        6 as decimals
    
    union all
    select 'Andromeda' as label, 
        0xB9373C557f3aE8cDdD068c1644ED226CfB18A997 as input, 
        0xc27C3D3130563C1171feCC4F76C217Db603997cf as jar, 
        0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 as currency, -- USDC
        6 as decimals
    union all
    select 'Paxos' as label, 
        0xda276ab5f1505965e0b6cd1b6da2a18ccbb29515 as input, 
        0x8bf8b5c58bb57ee9c97d0fea773eee042b10a787 as jar, 
        0x8e870d67f660d95d5be530380d0ec0bd388289e1 as currency, -- USDP
        18 as decimals
),
events as (
    select label, evt_block_time as ts, cast(value as decimal)/power(10, decimals) as input
    from erc20_ethereum.evt_Transfer
    cross join jars
    where contract_address = currency
        and to = input
)
select date_trunc('month', ts) as period, label,
    sum(input) as input
from events
group by 1, 2
order by 1 desc, 2 asc