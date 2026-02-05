WITH products(symbol, issuer, label, asset_type, underlying, underlying_link, trading_venue) as (
    VALUES
    ('BENJI', 'Franklin Templeton', 'Franklin OnChain U.S. Government Money Fund', 'Bonds', 'FOBXX'
        , 'https://www.franklintempleton.com/investments/options/money-market-funds/products/29386/SINGLCLASS/franklin-on-chain-u-s-government-money-fund/FOBXX'
        , null),
    ('BENJIv1', 'Franklin Templeton', 'Franklin OnChain U.S. Government Money Fund', 'Bonds', 'FOBXX'
        , 'https://www.franklintempleton.com/investments/options/money-market-funds/products/29386/SINGLCLASS/franklin-on-chain-u-s-government-money-fund/FOBXX'
        , null),


    ('MPLcashUSDC', 'Maple', 'Maple Cash Management Pool USDC', 'Bonds', 'T-Bills'
        , 'https://app.maple.finance/#/v2/lend/pool/0xfe119e9c24ab79f1bdd5dd884b86ceea2ee75d92'
        , null),
    ('MPLcashUSDT', 'Maple', 'Maple Cash Management Pool USDT', 'Bonds', 'T-Bills'
        , 'https://app.maple.finance/#/v2/lend/pool/0xf05681a33a9adf14076990789a89ab3da3f6b536'
        , null),

    ('tfBILL', 'TrueFi', 'TrueFi US Treasury Fund', 'Bonds', 'T-Bills'
        , 'https://app.archblock.com/offering/0x16D7d13B382D2b341B2b57D27118D71cB9f339e9'
        , null),

    ('USYC', 'Hashnote', 'Hashnote Short Duration Yield Coin', 'Bonds', 'T-Bills'
        , 'https://usyc.hashnote.com/'
        , null),

    ('BUIDL-I', 'BlackRock', 'BlackRock USD Institutional Digital Liquidity Fund - I Class', 'Bonds', 'T-Bills'
        , 'https://www.blackrock.com/cash/en-gb/products/229261/blackrock-ics-us-dollar-liquidity-premier-acc-fund'
        , null),
    ('BUIDL', 'BlackRock', 'BlackRock USD Institutional Digital Liquidity Fund - I Class', 'Bonds', 'T-Bills'
        , 'https://www.blackrock.com/cash/en-gb/products/229261/blackrock-ics-us-dollar-liquidity-premier-acc-fund'
        , null),

    ('TBILL', 'OpenEden', 'OpenEden T-Bills', 'Bonds', 'T-Bills'
        , 'https://app.openeden.com/tbill'
        , null)
    
)
, tokens as (
    select symbol, blockchain, token_address as contract_address
        , decimals, 0x0000000000000000000000000000000000000000 as mint_address, start_date
    from dune.steakhouse.result_token_info
    WHERE token_address in (
        0xa1e4e13c43eb3e24cde4cd15bb6d4b021b5bf79c, -- BENJI
        0x408A634B8a8f0dE729B48574a3a7Ec3fE820B00A, -- BENJI
        0x3DDc84940Ab509C11B20B76B466933f40b750dc9, -- BENJI
        0xb9e4765bce2609bc1949592059b17ea72fee6c6a, -- BENJI
        0xe08b4c1005603427420e64252a8b120cace4d122, -- BENJI
        0xfe119e9c24ab79f1bdd5dd884b86ceea2ee75d92, -- MPLcashUSDC
        0xf05681a33a9adf14076990789a89ab3da3f6b536, -- MPLcashUSDT
        0xA1F3aca66403D29b909605040C30ae1F1245d14c, -- tfBILL
        0x136471a34f6ef19fE571EFFC1CA711fdb8E49f2b, -- USYC
        0x8d0fa28f221eb5735bc71d3a0da67ee5bc821311, -- USYC
        0x6a9DA2D710BB9B700acde7Cb81F10F1fF8C89041, -- TBILL
        0x7712c34205737192402172409a8f7ccef8aa2aec, -- BUIDL
        0xa6525ae43edcd03dc08e775774dcabd3bb925872, -- BUIDL
        0xdd50C053C096CB04A3e3362E2b622529EC5f2e8a, -- TBILL
        0xF84D28A8D28292842dD73D1c5F99476A80b6666A -- TBILL
    )
)
SELECT issuer, symbol, blockchain as chain, contract_address, decimals, label, asset_type, underlying
    , underlying_link, trading_venue, mint_address, start_date
FROM products
    JOIN tokens using (symbol)
WHERE decimals is not null