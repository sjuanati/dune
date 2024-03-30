WITH tusd_trxns AS 
(

    SELECT "to"
    , "from"
    , CAST(value AS DOUBLE) AS value
    , evt_block_time
    FROM true_usd_ethereum.TrueUSD_evt_Transfer
), addresses AS (
    SELECT * FROM 
    (
        VALUES
        (0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be, 'CeFi', 'Binance')
        , (0xd551234ae421e3bcba99a0da6d736074f22192ff, 'CeFi', 'Binance')
        , (0x564286362092d8e7936f0549571a803b203aaced, 'CeFi', 'Binance')
        , (0x0681d8db095565fe8a346fa0277bffde9c0edbbf, 'CeFi', 'Binance')
        , (0xfe9e8709d3215310075d67e3ed32a380ccf451c8, 'CeFi', 'Binance')
        , (0x4e9ce36e442e55ecd9025b9a6e0d88485d628a67, 'CeFi', 'Binance')
        , (0xbe0eb53f46cd790cd13851d5eff43d12404d33e8, 'CeFi', 'Binance')
        , (0xf977814e90da44bfa03b6295a0616a897441acec, 'CeFi', 'Binance')
        , (0x001866ae5b3de6caa5a51543fd9fb64f524f5478, 'CeFi', 'Binance')
        , (0x85b931a32a0725be14285b66f1a22178c672d69b, 'CeFi', 'Binance')
        , (0x708396f17127c42383e3b9014072679b2f60b82f, 'CeFi', 'Binance')
        , (0x12392F67bdf24faE0AF363c24aC620a2f67DAd86, 'Lending', 'Compound')
        , (0xe0f0cfde7ee664943906f17f7f14342e76a5cec7, 'CeFi', 'Binance')
        , (0x8f22f2063d253846b53609231ed80fa571bc0c8f, 'CeFi', 'Binance')
        , (0x28c6c06298d514db089934071355e5743bf21d60, 'CeFi', 'Binance')
        , (0x21a31ee1afc51d94c2efccaa2092ad1028285549, 'CeFi', 'Binance')
        , (0xdfd5293d8e347dfe59e90efd55b2956a1343963d, 'CeFi', 'Binance')
        , (0x56eddb7aa87536c09ccc2793473599fd21a8b17f, 'CeFi', 'Binance')
        , (0x9696f59e4d72e237be84ffd425dcad154bf96976, 'CeFi', 'Binance')
        , (0xe79eef9b9388a4ff70ed7ec5bccd5b928ebb8bd1, 'CeFi', 'BitMart')
        , (0x68b22215ff74e3606bd5e6c1de8c2d68180c85f7, 'CeFi', 'BitMart')
        , (0x03bdf69b1322d623836afbd27679a1c0afa067e9, 'CeFi', 'BitMart')
        , (0x4b1a99467a284cc690e3237bc69105956816f762, 'CeFi', 'BitMart')
        , (0x986a2fca9eda0e06fbf7839b89bfc006ee2a23dd, 'CeFi', 'BitMart')
        , (0xe93381fb4c4f14bda253907b18fad305d799241a, 'CeFi', 'Huobi')
        , (0x2faf487a4414fe77e2327f0bf4ae2a264a776ad2, 'CeFi', 'FTX')
        , (0xc098b2a3aa256d2140208c3de6543aaef5cd3a94, 'CeFi', 'FTX ')
        , (0xfbb1b73c4f0bda4f67dca266ce6ef42f520fbb98, 'CeFi', 'Bittrex')
        , (0xe94b04a0fed112f3664e45adb2b8915693dd5ff3, 'CeFi', 'Bittrex')
        , (0x66f820a414680b5bcda5eeca5dea238543f42054, 'CeFi', 'Bittrex')
        , (0x2910543af39aba0cd09dbb2d50200b3e800a63d2, 'CeFi', 'Kraken')
        , (0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13, 'CeFi', 'Kraken')
        , (0xe853c56864a2ebe4576a807d26fdc4a0ada51919, 'CeFi', 'Kraken')
        , (0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0, 'CeFi', 'Kraken')
        , (0xfa52274dd61e1643d2205169732f29114bc240b3, 'CeFi', 'Kraken')
        , (0x89e51fa8ca5d66cd220baed62ed01e8951aa7c40, 'CeFi', 'Kraken')
        , (0x6cc5f688a315f3dc28a7781717a9a798a59fda7b, 'CeFi', 'OkEx')
        , (0x5f65f7b609678448494de4c87521cdf6cef1e932, 'CeFi', 'Gemini')
        , (0x07ee55aa48bb72dcc6e9d78256648910de513eca, 'CeFi', 'Gemini')
        , (0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8, 'CeFi', 'Gemini')
        , (0x61edcdf5bb737adffe5043706e7c5bb1f1a56eea, 'CeFi', 'Gemini')
        , (0x1151314c646ce4e0efd76d1af4760ae66a9fe30f, 'CeFi', 'Bitfinex')
        , (0x876eabf441b2ee5b5b0554fd502a8e0600950cfa, 'CeFi', 'Bitfinex')
        , (0xab7c74abc0c4d48d1bdad5dcb26153fc8780f83e, 'CeFi', 'Bitfinex')
        , (0xc6cde7c39eb2f0f0095f41570af89efc2c1ea828, 'CeFi', 'Bitfinex')
        , (0x6262998ced04146fa42253a5c0af90ca02dfd2a3, 'CeFi', 'Crypto.com')
        , (0x46340b20830761efd32832a74d7169b29feb9758, 'CeFi', 'Crypto.com')
        , (0x742d35cc6634c0532925a3b844bc454e4438f44e, 'CeFi', 'Crypto.com')
        , (0x0d0707963952f2fba59dd06f2b425ace40b492fe, 'CeFi', 'Gate.io')
        , (0x7793cd85c11a924478d358d49b05b37e91b5810f, 'CeFi', 'Gate.io')
        , (0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c, 'CeFi', 'Gate.io')
        , (0xd793281182a0e3e023116004778f45c29fc14f19, 'CeFi', 'Gate.io')
        , (0x2b5634c42055806a59e9107ed44d43c426e58258, 'CeFi', 'KuCoin')
        , (0x689c56aef474df92d44a1b70850f808488f9769c, 'CeFi', 'KuCoin')
        , (0xa1d8d972560c2f8144af871db508f0b0b10a3fbf, 'CeFi', 'KuCoin')
        , (0x4ad64983349c49defe8d7a4686202d24b25d0ce8, 'CeFi', 'KuCoin')
        , (0x236f9f97e0e62388479bf9e5ba4889e46b0273c3, 'CeFi', 'OKEx')
        , (0xa7efae728d2936e78bda97dc267687568dd593f3, 'CeFi', 'OKEx')
        , (0x5d3a536e4d6dbd6114cc1ead35777bab948e3643, 'Lending', 'Compound')
        , (0x028171bca77440897b824ca71d1c56cac55b68a3, 'Lending', 'Aave v2')
        , (0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7, 'Dex', 'Curve')
        , (0xc3d03e4f041fd4cd388c549ee2a29a9e5075882f, 'Dex', 'SushiSwap')
        , (0xa5407eae9ba41422680e2e00537571bcc53efbfd, 'Dex', 'Curve')
        , (0x8e595470ed749b85c6f7669de83eae304c2ec68f, 'Lending', 'Cream')
        , (0xa478c2975ab1ea89e8196811f51a7b7ade33eb11, 'Dex', 'Uniswap v2')
        , (0x3dfd23a6c5e8bbcfc9581d2e864a68feb6a076d3, 'Lending', 'Aave v1')
        , (0xd4405f0704621dbe9d4dea60e128e0c3b26bddbd, 'Dex', 'Uniswap v2')
        , (0x16cac1403377978644e78769daa49d8f6b6cf565, 'Dex', 'Balancer')
        , (0x16de59092dae5ccf4a1e6439d611fd0653f0bd01, 'Lending', 'Yearn')
        , (0xda816459f1ab5631232fe5e97a05bbbb94970c95, 'Lending', 'Yearn')
        , (0x6c6bc977e13df9b0de53b251522280bb72383700, 'Dex', 'Uniswap v3')
        , (0x19d3364a399d251e894ac732651be8b0e4e85001, 'Lending', 'Yearn')
        , (0x57755f7dec33320bca83159c26e93751bfd30fbe, 'Dex', 'Balancer')
        , (0x1e0447b19bb6ecfdae1e4ae1694b0c3659614e4e, 'Lending', 'DyDx')
        , (0x794e6e91555438afc3ccf1c5076a74f42133d08d, 'Dex', 'Oasis')
        , (0xc21d353ff4ee73c572425697f4f5aad2109fe35b, 'Lending', 'Alchemist')
        , (0xab7ae646063087317c1f410c6661364779f87d73, 'Dex', 'Bancor')
        , (0xacd43e627e64355f1861cec6d3a6688b31a6f952, 'Lending', 'Yearn')
        , (0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8, 'Dex', 'Uniswap v3')
        , (0x3fe7940616e5bc47b0775a0dccf6237893353bb4, 'Lending', 'Idle Cash')
        , (0xba12222222228d8ba445958a75a0704d566bf2c8, 'Dex', 'Balancer v2')
        , (0xfb76e9be55758d0042e003c1e46e186360f0627e, 'Other', 'Aragon')
        , (0x23773e65ed146a459791799d01336db287f25334, 'Other', 'Tornado Cash')
        , (0x9cd83be15a79646a3d22b81fc8ddf7b7240a62cb, 'Other', 'Pulsechain’s sacrifice')
        , (0xae461ca67b15dc8dc81ce7615e0320da1a9ab8d5, 'Dex', 'Uniswap v2')
        , (0x34d7d7aaf50ad4944b70b320acb24c95fa2def7c, 'Dex', 'SushiSwap') /* Ohm pool */
        , (0xa10c7ce4b876998858b1a9e12b10092229539400, 'Bridge', 'Arbitrum')
        , (0x40ec5b33f54e0e8a33a975908c5ba1c14e5bbbdf, 'Bridge', 'Polygon')
        , (0x467194771dae2967aef3ecbedd3bf9a310c76c65, 'Bridge', 'Optimism')
        , (0x12ed69359919fc775bc2674860e8fe2d2b6a7b5d, 'Bridge', 'RSK')
        , (0x4aa42145aa6ebf72e164c9bbc74fbd3788045016, 'Bridge', 'xDAI')
        , (0xdac7bb7ce4ff441a235f08408e632fa1d799a147, 'Bridge', 'Avalanche')
        , (0xc564ee9f21ed8a2d8e7e76c085740d5e4c5fafbe, 'Bridge', 'Anyswap: Fantom')
        , (0x9a8c4bdcd75cfa1059a6e453ac5ce9d3f5c82a35, 'Bridge', 'Fantom') /* Anyswap */
        , (0x13b432914a996b0a48695df9b2d701eda45ff264, 'Bridge', 'Nerve')
        , (0x23ddd3e3692d1861ed57ede224608875809e127f, 'Bridge', 'Near') /* Rainbow bridge */
        , (0x3014ca10b91cb3d0ad85fef7a3cb95bcac9c0f79, 'Bridge', 'Fuse')
        , (0xabea9132b05a70803a4e85094fd0e1800777fbef, 'Bridge', 'zkSync')
        , (0xa68d85df56e733a06443306a095646317b5fa633, 'Bridge', 'Hermez')
        , (0x5fdcca53617f4d2b9134b29090c87d01058e27e9, 'Bridge', 'Immutable X')
        , (0x2dccdb493827e15a5dc8f8b72147e6c4a5620857, 'Bridge', 'Harmony')
        , (0x070cb1270a4b2ba53c81cef89d0fd584ed4f430b, 'Bridge', 'OMG Network')
        , (0x737901bea3eeb88459df9ef1be8ff3ae1b42a2ba, 'Bridge', 'Aztec')
        , (0xe78388b4ce79068e89bf8aa7f218ef6b9ab0e9d0, 'Bridge', 'Avalanche')
        , (0x1bf68a9d1eaee7826b3593c20a0ca93293cb489a, 'Bridge', 'Orbit Chain')
        , (0x5777d92f208679db4b9778590fa3cab3ac9e2168, 'Dex', 'Uniswap v3')
        , (0x66c57bf505a85a74609d2c83e94aabb26d691e1f, 'CeFi', 'Kraken')
        , (0x47ac0fb4f2d84898e4d9e7b4dab3c24507a6d503, 'CeFi', 'Binance')
        , (0x5754284f345afc66a98fbb0a0afe71e0f007b949, 'CeFi', 'Bitfinex') /* usdt treasury for bitfinex */
        , (0x5041ed759dd4afc3a72b8192c143f72f4724081a, 'CeFi', 'OKEx') /* no dai in there as of rn */
        , (0xd6216fc19db775df9774a6e33526131da7d19a2c, 'CeFi', 'Kucoin')
        , (0xae2d4617c862309a3d75a0ffb358c7a5009c673f, 'CeFi', 'Kraken') /* pretty much just tusd in here */
        , (0xfdb16996831753d5331ff813c29a93c76834a0ad, 'CeFi', 'Huobi') /* no dai */
        , (0xeea81c4416d71cef071224611359f6f99a4c4294, 'CeFi', 'BitMEX') /* just usdt */
        , (0x0548f59fee79f8832c299e01dca5c76f034f558e, 'CeFi', 'Unspecified OTC Firm')
        , (0x6748f50f686bfbca6fe8ad62b22228b87f31ff2b, 'CeFi', 'Huobi') /* no dai */
        , (0xd1669ac6044269b59fa12c5822439f609ca54f41, 'CeFi', 'CoinList')
        , (0x8d1f2ebfaccf1136db76fdd1b86f1dede2d23852, 'CeFi', 'CoinList')
        , (0xd2c82f2e5fa236e114a81173e375a73664610998, 'CeFi', 'CoinList')
        , (0x0000006daea1723962647b7e189d311d757fb793, 'CeFi', 'Wintermute') /* centralized market maker that provides liquidity to both CEX and DEX */
        , (0x5a52e96bacdabb82fd05763e25335261b270efcb, 'CeFi', 'Binance')
        , (0xc57ae759c085c0d23a9bbf8cd3e3d306a0acf7db, 'CeFi', 'Huobi')
        , (0xda9dfa130df4de4673b89022ee50ff26f6ea73cf, 'CeFi', 'Kraken')
        , (0xc882b111a75c0c657fc507c04fbfcd2cc984f071, 'CeFi', 'Gate.io')
        , (0x77134cbc06cb00b66f4c7e623d5fdbf6777635ec, 'CeFi', 'Bitfinex')
        , (0xec30d02f10353f8efc9601371f56e808751f396f, 'CeFi', 'Kucoin')
        , (0xf89d7b9c864f589bbf53a82105107622b35eaa40, 'CeFi', 'Unknown')
        , (0xa910f92acdaf488fa6ef02174fb86208ad7722ba, 'CeFi', 'Poloniex')
        , (0x7abe0ce388281d2acf297cb089caef3819b13448, 'CeFi', 'Unknown')
        , (0x187c0e0aa33282096b39a33457939f1dc3ea8e0f, 'CeFi', 'Coinlist')
        , (0xcffad3200574698b78f32232aa9d63eabd290703, 'CeFi', 'Unknown')
        , (0x2c8fbb630289363ac80705a1a61273f76fd5a161, 'CeFi', 'OKEx')
        , (0x88bd4d3e2997371bceefe8d9386c6b5b4de60346, 'CeFi', 'Kucoin')
        , (0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43, 'CeFi', 'Coinbase')
        , (0x8d6f396d210d385033b348bcae9e4f9ea4e045bd, 'CeFi', 'Gemini')
        , (0xb60c61dbb7456f024f9338c739b02be68e3f545c, 'CeFi', 'Nexo')
        , (0xf16e9b0d03470827a95cdfd0cb8a8a3b46969b91, 'CeFi', 'Kucoin')
        , (0x30741289523c2e4d2a62c7d6722686d14e723851, 'CeFi', 'Unknown')
        , (0xffec0067f5a79cff07527f63d83dd5462ccf8ba4, 'CeFi', 'Nexo')
        , (0x9aa65464b4cfbe3dc2bdb3df412aee2b3de86687, 'CeFi', 'Unknown')
        , (0x5c985e89dde482efe97ea9f1950ad149eb73829b, 'CeFi', 'Huobi')
        , (0xb8689b7910954BF73431f63482D7dd155537ea7E, 'Treasury', 'Dopex Multisig')
        , (0x3Fe6a295459FAe07DF8A0ceCC36F37160FE86AA9, 'Lending', 'Aave Ethereum: LUSD')
        , (0x57891966931Eb4Bb6FB81430E6cE0A03AAbDe063, 'Bridge', 'zkSync Era: Bridge')
        , (0x7b065Fcb0760dF0CEA8CFd144e08554F3CeA73D1, 'Treasury', 'Gearbox: Treasury')
        , (0x6ABfd6139c7C3CC270ee2Ce132E309F59cAaF6a2, 'Treasury', 'Morpho')
        , (0x462A63D4405A6462b157341A78Fd1baBfD3F8065, 'Treasury', 'Index Protocol')
        , (0x90A48D5CF7343B08dA12E067680B4C6dbfE551Be, 'Treasury', 'Shapeshift')
        , (0xC131701Ea649AFc0BfCc085dc13304Dc0153dc2e, 'CeFi', 'Celsius Network')
        , (0xEcd5e75AFb02eFa118AF914515D6521aaBd189F1, 'DEX', 'Curve.Fi TUSDCRV-f')
        , (0xA929022c9107643515F5c777cE9a910F0D1e490C, 'Bridge', 'Heco Chain: Bridge')
    ) AS wallets(wallet, wallet_type, protocol)
), addresses2 AS
(
    SELECT * FROM addresses
    
    UNION ALL
    
    SELECT address AS wallet, 'CeFi' AS wallet_type, cex_name AS protocol
    FROM cex.addresses 
    WHERE address NOT IN (SELECT wallet FROM addresses)
    
    UNION ALL
    
    SELECT address AS wallet, 'Bridge' AS wallet_type, bridge_name AS protocol
    FROM addresses_ethereum.bridges
    WHERE address NOT IN (SELECT wallet FROM addresses)
), additional_contracts AS 
(
    SELECT * FROM
    (
        VALUES 
        (0x12ed69359919fc775bc2674860e8fe2d2b6a7b5d, 'RSK', 'Bridge')
        , (0x23773e65ed146a459791799d01336db287f25334, 'Tornado Cash', 'Other')
        , (0x9a8c4bdcd75cfa1059a6e453ac5ce9d3f5c82a35, 'Fantom', 'Bridge')
        , (0xa68d85df56e733a06443306a095646317b5fa633, 'Hermez', 'Bridge')
        , (0x1bf68a9d1eaee7826b3593c20a0ca93293cb489a, 'Orbit Chain', 'Bridge')
        , (0x9cd83be15a79646a3d22b81fc8ddf7b7240a62cb, 'Pulsechain Sacrifice', 'Other')
        , (0xdac7bb7ce4ff441a235f08408e632fa1d799a147, 'Avalanche', 'Bridge')
        , (0x13b432914a996b0a48695df9b2d701eda45ff264, 'Nerve', 'Bridge')
        , (0x3014ca10b91cb3d0ad85fef7a3cb95bcac9c0f79, 'Fuse', 'Bridge')
        , (0xa10c7ce4b876998858b1a9e12b10092229539400, 'Arbitrum', 'Bridge')
        , (0xe78388b4ce79068e89bf8aa7f218ef6b9ab0e9d0, 'Avalanche', 'Bridge')
        , (0xc564ee9f21ed8a2d8e7e76c085740d5e4c5fafbe, 'Multichain Fantom', 'Bridge')
        , (0x467194771dae2967aef3ecbedd3bf9a310c76c65, 'Optimism', 'Bridge')
        , (0x1116898dda4015ed8ddefb84b6e8bc24528af2d8, 'Synapse Stableswap', 'DEX')/* has a bridge too but separate contract */
        , (0xec4486a90371c9b66f499ff3936f29f0d5af8b7e, 'Multichain Moonbeam', 'Bridge')
        , (0x0a59649758aa4d66e25f08dd01271e891fe52199, 'Maker tusd PSM', 'PSM')
        , (0x66017D22b0f8556afDd19FC67041899Eb65a21bb, 'Liquity Stability Pool', 'Stability Pool')
        , (0xa929022c9107643515f5c777ce9a910f0d1e490c, 'HECO Chain', 'Bridge')
        , (0xbbc4a8d076f4b1888fec42581b6fc58d242cf2d5, 'anyMIM Token', 'Bridge') /* just for MIM, token used for bridge purposes */
        , (0xcee284f754e854890e311e3280b767f80797180d, 'Arbitrum', 'Bridge')/* no dai in this bridge */
        , (0x7bbd8ca5e413bca521c2c80d8d1908616894cf21, 'Maker USDP PSM', 'PSM')
        , (0x7e62b7e279dfc78deb656e34d6a435cc08a44666, 'Maker USDP Vault', 'Lending')
        , (0xa191e578a6736167326d05c119ce0c90849e84b7, 'Maker tusd Vault', 'Lending')
        , (0x10c6b61dbf44a083aec3780acf769c77be747e23, 'Multichain Moonriver', 'Bridge')
        , (0xa57bd00134b2850b2a1c55860c9e9ea100fdd6cf, 'Someones MEV Bot', 'MEV Bot')
        , (0xfd3dfb524b2da40c8a6d703c62be36b5d8540626, '1inch', 'DEX')
        , (0xe069cb01d06ba617bcdf789bf2ff0d5e5ca20c71, '1inch', 'DEX')
        , (0xdb38ae75c5f44276803345f7f02e95a0aeef5944, '1inch', 'DEX')
        , (0x220bda5c8994804ac96ebe4df184d25e5c2196d4, '1inch', 'DEX')
        , (0x27239549dd40e1d60f5b80b0c4196923745b1fd2, '1inch', 'DEX')
        , (0x2057cfb9fd11837d61b294d514c5bd03e5e7189a, '1inch', 'DEX')
        , (0x58a3c68e2d3aaf316239c003779f71acb870ee47, 'curvefi', 'DEX')
        , (0x74de5d4fcbf63e00296fd95d33236b9794016631, 'airswap', 'DEX')
        , (0xc80573c8d53ea1bba1ed505bbb537dcd4adb9067, 'vesper', 'Yield Farm')
        , (0xbf3f6477dbd514ef85b7d3ec6ac2205fd0962039, 'Someones MEV Bot', 'MEV Bot')
        , (0xa8ecaf8745c56d5935c232d2c5b83b9cd3de1f6a, 'Someones MEV Bot', 'MEV Bot')
        , (0x561b94454b65614ae3db0897b74303f4acf7cc75, 'zeroex', 'DEX')
        , (0x7a6f6a048fe2dc1397aba0bf7879d3eacf371c53, 'zeroex', 'DEX')
        , (0xa2033d6ba88756ce6a87584d69dc87bda9a4f889, 'zeroex', 'DEX')
        , (0xc176761d388caf2f56cf03329d82e1e7c48ae09c, '1inch', 'DEX')
        , (0x7566126f2fd0f2dddae01bb8a6ea49b760383d5a, '1inch', 'DEX')
        , (0x894d1ca4c14fa5729641c4bdc461431913422b02, 'alpha finance zap into sushi LP', 'DEX')
        , (0x166a309efceedc82c501014130beadd0b3097475, 'Someones MEV Bot', 'MEV Bot')
        , (0x0000000000007f150bd6f54c40a34d7c3d5e9f56, 'Someones MEV Bot', 'MEV Bot')
        , (0x8eb8a3b98659cce290402893d0123abb75e3ab28, 'Avalanche', 'Bridge')
        , (0x8ffae111ab06f532a18418190129373d14570014, 'Fei Treasury', 'Treasury')
        , (0x88ad09518695c6c3712ac10a214be5109a655671, 'Gnosis Bridge', 'Bridge')
        , (0xe61dd9ca7364225afbfb79e15ad33864424e6ae4, 'Unknown Bridge', 'Bridge')
        , (0x56178a0d5f301baf6cf3e1cd53d9863437345bf9, 'Someones MEV Bot', 'MEV Bot')
        , (0xf861483fa7e511fbc37487d91b6faa803af5d37c, 'curvefi', 'DEX')
        , (0x03b59bd1c8b9f6c265ba0c3421923b93f15036fa, 'fraxswap', 'DEX')
        , (0x13cc34aa8037f722405285ad2c82fe570bfa2bdc, 'saddle', 'DEX')
        , (0x8c240c385305aeb2d5ceb60425aabcb3488fa93d, 'UwUlend', 'Lending')
        , (0xb95bd0793bcc5524af358ffaae3e38c3903c7626, 'UwUlend', 'Lending')
        , (0xc480a11a524e4db27c6d4e814b4d9b3646bc12fc, 'UwUlend', 'Lending')
        , (0xadfa5fa0c51d11b54c8a0b6a15f47987bd500086, 'UwUlend', 'Lending')
        , (0xd79886841026a39cff99321140b3c4d31314782b, 'fraxswap', 'DEX')
        , (0x56695c26b3cdb528815cd22ff7b47510ab821efd, 'fraxswap', 'DEX')
        , (0x31351bf3fba544863fbff44ddc27ba880916a199, 'fraxswap', 'DEX')
        , (0x0a92ac70b5a187fb509947916a8f63dd31600f80, 'fraxswap', 'DEX')
        , (0xcb0bc7c879bb3e9cfeb9d8efef653f33b3d242e9, 'fraxswap', 'DEX')
        , (0x750bb20608601e4c44acbe774fac8f37dab67c86, 'fraxswap', 'DEX')
        , (0x533e3c0e6b48010873b947bddc4721b1bdff9648, 'Multichain BSC Old', 'Bridge')
        , (0x5769071665eb8db80e7e9226f92336bb2897dcfa, 'fraxswap', 'DEX')
        , (0x832c6f546bf34a552deb8773216a93bf6801028c, 'fraxswap', 'DEX')
        , (0x4f60a160d8c2dddaafe16fcc57566db84d674bd6, 'anyFRAX', 'Bridge')
        , (0x5e8c1ad4c3d04f059d5c1a2ce4593e53be270bca, 'fraxswap', 'DEX')
        , (0x000000000dfde7deaf24138722987c9a6991e2d4, 'Someones MEV Bot', 'MEV Bot')
        , (0x9a22cdb1ca1cdd2371cd5bb5199564c4e89465eb, 'curvefi', 'DEX')
        , (0x07af6bb51d6ad0cf126e3ed2dee6eac34bf094f8, 'fraxswap', 'DEX')
    ) AS t (address, namespace, name)
), contracts AS 
(
    SELECT
    address
    , namespace
    , name
    FROM
    (
        SELECT *
        , ROW_NUMBER() OVER (PARTITION BY address ORDER BY created_at) AS rn
        FROM ethereum.contracts
        WHERE address IS NOT NULL
        AND address NOT IN (SELECT address FROM additional_contracts)
        AND address NOT IN (SELECT wallet FROM addresses2 WHERE wallet_type = 'CeFi')
    ) AS sub
    WHERE rn = 1
    UNION ALL
    SELECT * FROM additional_contracts
), contracts2 AS 
(
    SELECT address
    , namespace
    , name
    , CASE WHEN name IN ('OlympusTreasury','MapleTreasury','Proxy','Proxy_v2','Treasury') THEN 'Treasury' /* potentially includes personal smart contract wallets */
        WHEN name IN ('Pair','Fei3Crv','LiquidityPoolV4','DMMPool','Curve','UniswapV2Pair','DEX','YPoolDelegator','AMMWrapper','SigThreePool') THEN 'DEX'
        -- WHEN name IN ('FixedPricePSM', 'PSM', 'DssPsm') THEN 'PSM' /* psm technically, keeping it isolated in case I want to change */
        WHEN name IN ('Stability Pool') THEN 'Stability Pool'
        WHEN name IN ('CErc20Delegator','cErc20','CErc20Delegate','StabilityPool','TransmuterB','PoolService','ironbank','Lending','Alchemist') THEN 'Lending' /* first 3 cover compound and its forks */
        WHEN name IN ('LiquidityStakingV1', 'SoloMargin') THEN 'Lending' /* dydx */
        WHEN name IN ('Bridge','L1StandardBridge','TokenBridge','OVM_L1StandardBridge','BridgeRouter','L1_ERC20_Bridge','BridgePoolProd'
            ,'L1ERC20Gateway','Wormhole','xdai_bridge','Erc20Vault','StarkPerpetual','StarkExchange') THEN 'Bridge'
        WHEN name IN ('DegenBox', 'BentoBoxV1', 'Yield Farm') THEN 'Yield Farm'
        WHEN name IN ('MEV Bot') THEN 'MEV Bot'
        WHEN name IN ('InstaFlashAggregator') THEN 'Lending'
        WHEN namespace IN ('gnosis_safe','aragon','gnosis_multisig','reserve_protocol','endaoment') THEN 'Treasury' /* reserve protocol is a treasury that backs their stablecoin, kind of diff. */
        WHEN namespace IN ('curvefi','balancer_v2','saddle','dodo','bancor','bancor3','balancer','defiswap','temple','shell_v1'
            ,'oasisdex','zeroex_v3','gnosis_protocol_v2','oneinch','paraswap','integral','oasis_dex') THEN 'DEX'
        WHEN namespace IN ('aave_v2','truefi','maple','euler','aave','ethichub','yield_v2','premia','notional_v2') THEN 'Lending' /* premia is an options pool, which is a type of lending with risk (lending to allow options to be underwritten) */
        WHEN namespace IN ('nexusmutual', 'insurace') THEN 'Lending' /* most of the funds in here are lent to underwrite insurance (lending w risk), so will lump in with lending for now. Similar with premia (& options) above */
        WHEN namespace IN ('matic','near','celer','zksync','poly_network','force_bridge','aztec_v1','stargate') THEN 'Bridge'
        WHEN namespace IN ('yearn','yearn_v2','SavingsContract','iearn_v2','idle_v4','pooltogether','pooltogether_v1'
            ,'pooltogether_v2','pooltogether_v3','pooltogether_v4') THEN 'Yield Farm'
        ELSE 'Other'
    END AS grpd
    FROM contracts
), contracts_and_cefi AS 
(
    SELECT * FROM contracts2
    UNION ALL
    SELECT wallet AS address
    , protocol AS namespace
    , protocol AS name
    , wallet_type AS grpd
    FROM addresses2
    WHERE wallet_type IN ('CeFi')
), deltas AS 
(
    SELECT "to" AS address
    , DATE(evt_block_time) AS dt
    , value AS delta
    FROM tusd_trxns
    UNION ALL
    SELECT "from" AS address
    , DATE(evt_block_time) AS dt
    , -value AS delta
    FROM tusd_trxns
), deltas2 AS
(
    SELECT COALESCE(CASE WHEN grpd IS NULL THEN 'EOA' ELSE grpd END,'Other') AS wallet
    , address
    , deltas.dt
    , deltas.delta
    FROM deltas
    LEFT JOIN contracts_and_cefi
    USING (address)
    WHERE address NOT IN (0x0000000000000000000000000000000000000000, 0x55fe002aeff02f77364de339a1292923a15844b8) /* burn address, circle address */
    
    UNION ALL
    
    SELECT wallet
    , NULL AS address
    , dt
    , 0 AS delta
    FROM (SELECT grpd AS wallet FROM contracts_and_cefi GROUP BY 1 UNION SELECT 'EOA' UNION SELECT 'Other')
    CROSS JOIN 
    UNNEST(SEQUENCE(DATE('2018-12-31'), CURRENT_DATE, INTERVAL '1' DAY)) AS t(dt)
), grouped AS 
(
    SELECT wallet
    , dt
    , SUM(delta) AS delta
    FROM deltas2
    GROUP BY 1, 2
), balances AS 
(
    SELECT wallet
    , dt
    , SUM(delta) OVER (PARTITION BY wallet ORDER BY dt) / 1e18 AS balance
    FROM grouped
)
SELECT dt, wallet, balance, total_balance
FROM (
    SELECT wallet
    , dt
    , balance
    , SUM(balance) OVER (PARTITION BY dt) AS total_balance
    FROM balances
    WHERE dt > DATE('2018-12-31') 
    -- and balance > 1e-6
    ORDER BY dt DESC NULLS FIRST

)
WHERE dt > DATE('2020-06-01')
and balance > 1e-6
