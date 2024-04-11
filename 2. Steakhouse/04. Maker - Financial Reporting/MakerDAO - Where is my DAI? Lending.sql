WITH
  addresses AS (
    SELECT
      0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be AS wallet,
      'CeFi' AS wallet_type,
      'Binance' AS protocol
    UNION ALL
    SELECT
      0xd551234ae421e3bcba99a0da6d736074f22192ff AS wallet,
      'CeFi' AS wallet_type,
      'Binance' AS protocol
    UNION ALL
    SELECT
      0x564286362092d8e7936f0549571a803b203aaced AS wallet,
      'CeFi' AS wallet_type,
      'Binance' AS protocol
    UNION ALL
    SELECT
      0x0681d8db095565fe8a346fa0277bffde9c0edbbf AS wallet,
      'CeFi' AS wallet_type,
      'Binance' AS protocol
    UNION ALL
    SELECT
      0xfe9e8709d3215310075d67e3ed32a380ccf451c8 AS wallet,
      'CeFi' AS wallet_type,
      'Binance' AS protocol
    UNION ALL
    SELECT
      0x4e9ce36e442e55ecd9025b9a6e0d88485d628a67 AS wallet,
      'CeFi' AS wallet_type,
      'Binance' AS protocol
    UNION ALL
    SELECT
      0xbe0eb53f46cd790cd13851d5eff43d12404d33e8 AS wallet,
      'CeFi' AS wallet_type,
      'Binance' AS protocol
    UNION ALL
    SELECT
      0xf977814e90da44bfa03b6295a0616a897441acec AS wallet,
      'CeFi' AS wallet_type,
      'Binance' AS protocol
    UNION ALL
    SELECT
      0x001866ae5b3de6caa5a51543fd9fb64f524f5478 AS wallet,
      'CeFi' AS wallet_type,
      'Binance' AS protocol
    UNION ALL
    SELECT
      0x85b931a32a0725be14285b66f1a22178c672d69b AS wallet,
      'CeFi' AS wallet_type,
      'Binance' AS protocol
    UNION ALL
    SELECT
      0x708396f17127c42383e3b9014072679b2f60b82f AS wallet,
      'CeFi' AS wallet_type,
      'Binance' AS protocol
    UNION ALL
    SELECT
      0xe0f0cfde7ee664943906f17f7f14342e76a5cec7 AS wallet,
      'CeFi' AS wallet_type,
      'Binance' AS protocol
    UNION ALL
    SELECT
      0x8f22f2063d253846b53609231ed80fa571bc0c8f AS wallet,
      'CeFi' AS wallet_type,
      'Binance' AS protocol
    UNION ALL
    SELECT
      0x28c6c06298d514db089934071355e5743bf21d60 AS wallet,
      'CeFi' AS wallet_type,
      'Binance' AS protocol
    UNION ALL
    SELECT
      0x21a31ee1afc51d94c2efccaa2092ad1028285549 AS wallet,
      'CeFi' AS wallet_type,
      'Binance' AS protocol
    UNION ALL
    SELECT
      0xdfd5293d8e347dfe59e90efd55b2956a1343963d AS wallet,
      'CeFi' AS wallet_type,
      'Binance' AS protocol
    UNION ALL
    SELECT
      0x56eddb7aa87536c09ccc2793473599fd21a8b17f AS wallet,
      'CeFi' AS wallet_type,
      'Binance' AS protocol
    UNION ALL
    SELECT
      0x9696f59e4d72e237be84ffd425dcad154bf96976 AS wallet,
      'CeFi' AS wallet_type,
      'Binance' AS protocol
    UNION ALL
    SELECT
      0xe79eef9b9388a4ff70ed7ec5bccd5b928ebb8bd1 AS wallet,
      'CeFi' AS wallet_type,
      'BitMart' AS protocol
    UNION ALL
    SELECT
      0x68b22215ff74e3606bd5e6c1de8c2d68180c85f7 AS wallet,
      'CeFi' AS wallet_type,
      'BitMart' AS protocol
    UNION ALL
    SELECT
      0x03bdf69b1322d623836afbd27679a1c0afa067e9 AS wallet,
      'CeFi' AS wallet_type,
      'BitMart' AS protocol
    UNION ALL
    SELECT
      0x4b1a99467a284cc690e3237bc69105956816f762 AS wallet,
      'CeFi' AS wallet_type,
      'BitMart' AS protocol
    UNION ALL
    SELECT
      0x986a2fca9eda0e06fbf7839b89bfc006ee2a23dd AS wallet,
      'CeFi' AS wallet_type,
      'BitMart' AS protocol
    UNION ALL
    SELECT
      0xe93381fb4c4f14bda253907b18fad305d799241a AS wallet,
      'CeFi' AS wallet_type,
      'Huobi' AS protocol
    UNION ALL
    SELECT
      0x2faf487a4414fe77e2327f0bf4ae2a264a776ad2 AS wallet,
      'CeFi' AS wallet_type,
      'FTX' AS protocol
    UNION ALL
    SELECT
      0xc098b2a3aa256d2140208c3de6543aaef5cd3a94 AS wallet,
      'CeFi' AS wallet_type,
      'FTX ' AS protocol
    UNION ALL
    SELECT
      0xfbb1b73c4f0bda4f67dca266ce6ef42f520fbb98 AS wallet,
      'CeFi' AS wallet_type,
      'Bittrex' AS protocol
    UNION ALL
    SELECT
      0xe94b04a0fed112f3664e45adb2b8915693dd5ff3 AS wallet,
      'CeFi' AS wallet_type,
      'Bittrex' AS protocol
    UNION ALL
    SELECT
      0x66f820a414680b5bcda5eeca5dea238543f42054 AS wallet,
      'CeFi' AS wallet_type,
      'Bittrex' AS protocol
    UNION ALL
    SELECT
      0x2910543af39aba0cd09dbb2d50200b3e800a63d2 AS wallet,
      'CeFi' AS wallet_type,
      'Kraken' AS protocol
    UNION ALL
    SELECT
      0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13 AS wallet,
      'CeFi' AS wallet_type,
      'Kraken' AS protocol
    UNION ALL
    SELECT
      0xe853c56864a2ebe4576a807d26fdc4a0ada51919 AS wallet,
      'CeFi' AS wallet_type,
      'Kraken' AS protocol
    UNION ALL
    SELECT
      0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0 AS wallet,
      'CeFi' AS wallet_type,
      'Kraken' AS protocol
    UNION ALL
    SELECT
      0xfa52274dd61e1643d2205169732f29114bc240b3 AS wallet,
      'CeFi' AS wallet_type,
      'Kraken' AS protocol
    UNION ALL
    SELECT
      0x89e51fa8ca5d66cd220baed62ed01e8951aa7c40 AS wallet,
      'CeFi' AS wallet_type,
      'Kraken' AS protocol
    UNION ALL
    SELECT
      0x6cc5f688a315f3dc28a7781717a9a798a59fda7b AS wallet,
      'CeFi' AS wallet_type,
      'OkEx' AS protocol
    UNION ALL
    SELECT
      0x5f65f7b609678448494de4c87521cdf6cef1e932 AS wallet,
      'CeFi' AS wallet_type,
      'Gemini' AS protocol
    UNION ALL
    SELECT
      0x07ee55aa48bb72dcc6e9d78256648910de513eca AS wallet,
      'CeFi' AS wallet_type,
      'Gemini' AS protocol
    UNION ALL
    SELECT
      0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8 AS wallet,
      'CeFi' AS wallet_type,
      'Gemini' AS protocol
    UNION ALL
    SELECT
      0x61edcdf5bb737adffe5043706e7c5bb1f1a56eea AS wallet,
      'CeFi' AS wallet_type,
      'Gemini' AS protocol
    UNION ALL
    SELECT
      0x1151314c646ce4e0efd76d1af4760ae66a9fe30f AS wallet,
      'CeFi' AS wallet_type,
      'Bitfinex' AS protocol
    UNION ALL
    SELECT
      0x876eabf441b2ee5b5b0554fd502a8e0600950cfa AS wallet,
      'CeFi' AS wallet_type,
      'Bitfinex' AS protocol
    UNION ALL
    SELECT
      0xab7c74abc0c4d48d1bdad5dcb26153fc8780f83e AS wallet,
      'CeFi' AS wallet_type,
      'Bitfinex' AS protocol
    UNION ALL
    SELECT
      0xc6cde7c39eb2f0f0095f41570af89efc2c1ea828 AS wallet,
      'CeFi' AS wallet_type,
      'Bitfinex' AS protocol
    UNION ALL
    SELECT
      0x6262998ced04146fa42253a5c0af90ca02dfd2a3 AS wallet,
      'CeFi' AS wallet_type,
      'Crypto.com' AS protocol
    UNION ALL
    SELECT
      0x46340b20830761efd32832a74d7169b29feb9758 AS wallet,
      'CeFi' AS wallet_type,
      'Crypto.com' AS protocol
    UNION ALL
    SELECT
      0x742d35cc6634c0532925a3b844bc454e4438f44e AS wallet,
      'CeFi' AS wallet_type,
      'Crypto.com' AS protocol
    UNION ALL
    SELECT
      0x0d0707963952f2fba59dd06f2b425ace40b492fe AS wallet,
      'CeFi' AS wallet_type,
      'Gate.io' AS protocol
    UNION ALL
    SELECT
      0x7793cd85c11a924478d358d49b05b37e91b5810f AS wallet,
      'CeFi' AS wallet_type,
      'Gate.io' AS protocol
    UNION ALL
    SELECT
      0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c AS wallet,
      'CeFi' AS wallet_type,
      'Gate.io' AS protocol
    UNION ALL
    SELECT
      0xd793281182a0e3e023116004778f45c29fc14f19 AS wallet,
      'CeFi' AS wallet_type,
      'Gate.io' AS protocol
    UNION ALL
    SELECT
      0x2b5634c42055806a59e9107ed44d43c426e58258 AS wallet,
      'CeFi' AS wallet_type,
      'KuCoin' AS protocl
    UNION ALL
    SELECT
      0x689c56aef474df92d44a1b70850f808488f9769c AS wallet,
      'CeFi' AS wallet_type,
      'KuCoin' AS protocl
    UNION ALL
    SELECT
      0xa1d8d972560c2f8144af871db508f0b0b10a3fbf AS wallet,
      'CeFi' AS wallet_type,
      'KuCoin' AS protocl
    UNION ALL
    SELECT
      0x4ad64983349c49defe8d7a4686202d24b25d0ce8 AS wallet,
      'CeFi' AS wallet_type,
      'KuCoin' AS protocl
    UNION ALL
    SELECT
      0x236f9f97e0e62388479bf9e5ba4889e46b0273c3 AS wallet,
      'CeFi' AS wallet_type,
      'OKEx' AS protocol
    UNION ALL
    SELECT
      0xa7efae728d2936e78bda97dc267687568dd593f3 AS wallet,
      'CeFi' AS wallet_type,
      'OKEx' AS protocol
    UNION ALL
    SELECT
      0x5d3a536e4d6dbd6114cc1ead35777bab948e3643 AS wallet,
      'Lending' AS wallet_type,
      'Compound' AS protocol
    UNION ALL
    SELECT
      0x028171bca77440897b824ca71d1c56cac55b68a3 AS wallet,
      'Lending' AS wallet_type,
      'Aave v2' AS protocol
    UNION ALL
    SELECT
      0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7 AS wallet,
      'Dex' AS wallet_type,
      'Curve' AS protocol
    UNION ALL
    SELECT
      0xc3d03e4f041fd4cd388c549ee2a29a9e5075882f AS wallet,
      'Dex' AS wallet_type,
      'SushiSwap' AS protocol
    UNION ALL
    SELECT
      0xa5407eae9ba41422680e2e00537571bcc53efbfd AS wallet,
      'Dex' AS wallet_type,
      'Curve' AS protocol
    UNION ALL
    SELECT
      0x8e595470ed749b85c6f7669de83eae304c2ec68f AS wallet,
      'Lending' AS wallet_type,
      'Cream' AS protocol
    UNION ALL
    SELECT
      0xa478c2975ab1ea89e8196811f51a7b7ade33eb11 AS wallet,
      'Dex' AS wallet_type,
      'Uniswap v2' AS protocol
    UNION ALL
    SELECT
      0x3dfd23a6c5e8bbcfc9581d2e864a68feb6a076d3 AS wallet,
      'Lending' AS wallet_type,
      'Aave v1' AS protocol
    UNION ALL
    SELECT
      0xd4405f0704621dbe9d4dea60e128e0c3b26bddbd AS wallet,
      'Dex' AS wallet_type,
      'Uniswap v2' AS protocol
    UNION ALL
    SELECT
      0x16cAC1403377978644e78769Daa49d8f6B6CF565 AS wallet,
      'Dex' AS wallet_type,
      'Balancer' AS protocol
    UNION ALL
    SELECT
      0x16de59092dae5ccf4a1e6439d611fd0653f0bd01 AS wallet,
      'Lending' AS wallet_type,
      'Yearn' AS protocol
    UNION ALL
    SELECT
      0xda816459f1ab5631232fe5e97a05bbbb94970c95 AS wallet,
      'Lending' AS wallet_type,
      'Yearn' AS protocol
    UNION ALL
    SELECT
      0x6c6bc977e13df9b0de53b251522280bb72383700 AS wallet,
      'Dex' AS wallet_type,
      'Uniswap v3' AS protocol
    UNION ALL
    SELECT
      0x19d3364a399d251e894ac732651be8b0e4e85001 AS wallet,
      'Lending' AS wallet_type,
      'Yearn' AS protocol
    UNION ALL
    SELECT
      0x57755f7dec33320bca83159c26e93751bfd30fbe AS wallet,
      'Dex' AS wallet_type,
      'Balancer' AS protocol
    UNION ALL
    SELECT
      0x1e0447b19bb6ecfdae1e4ae1694b0c3659614e4e AS wallet,
      'Lending' AS wallet_type,
      'DyDx' AS protocol
    UNION ALL
    SELECT
      0x794e6e91555438afc3ccf1c5076a74f42133d08d AS wallet,
      'Dex' AS wallet_type,
      'Oasis' AS protocol
    UNION ALL
    SELECT
      0xc21d353ff4ee73c572425697f4f5aad2109fe35b AS wallet,
      'Lending' AS wallet_type,
      'Alchemist' AS protocol
    UNION ALL
    SELECT
      0xab7ae646063087317c1f410c6661364779f87d73 AS wallet,
      'Dex' AS wallet_type,
      'Bancor' AS protocol
    UNION ALL
    SELECT
      0xacd43e627e64355f1861cec6d3a6688b31a6f952 AS wallet,
      'Lending' AS wallet_type,
      'Yearn' AS protocol
    UNION ALL
    SELECT
      0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8 AS wallet,
      'Dex' AS wallet_type,
      'Uniswap v3' AS protocol
    UNION ALL
    SELECT
      0x3fe7940616e5bc47b0775a0dccf6237893353bb4 AS wallet,
      'Lending' AS wallet_type,
      'Idle Cash' AS protocol
    UNION ALL
    SELECT
      0xba12222222228d8ba445958a75a0704d566bf2c8 AS wallet,
      'Dex' AS wallet_type,
      'Balancer v2' AS protocol
    UNION ALL
    SELECT
      0xfb76e9be55758d0042e003c1e46e186360f0627e AS wallet,
      'Other' AS wallet_type,
      'Aragon' AS protocol
    UNION ALL
    SELECT
      0x23773e65ed146a459791799d01336db287f25334 AS wallet,
      'Other' AS wallet_type,
      'Tornado Cash' AS protocol
    UNION ALL
    SELECT
      0x9cd83be15a79646a3d22b81fc8ddf7b7240a62cb AS wallet,
      'Other' AS wallet_type,
      'Pulsechain’s sacrifice' AS protocol
    UNION ALL
    SELECT
      0xae461ca67b15dc8dc81ce7615e0320da1a9ab8d5 AS wallet,
      'Dex' AS wallet_type,
      'Uniswap v2' AS protocol
    UNION ALL
    SELECT
      0x34d7d7aaf50ad4944b70b320acb24c95fa2def7c AS wallet,
      'Dex' AS wallet_type,
      'SushiSwap' AS protocol /* Ohm pool */
    UNION ALL
    SELECT
      0xA10c7CE4b876998858b1a9E12b10092229539400 AS wallet,
      'Bridge' AS wallet_type,
      'Arbitrum' AS protocol
    UNION ALL
    SELECT
      0x40ec5b33f54e0e8a33a975908c5ba1c14e5bbbdf AS wallet,
      'Bridge' AS wallet_type,
      'Polygon' AS protocol
    UNION ALL
    SELECT
      0x467194771dAe2967Aef3ECbEDD3Bf9a310C76C65 AS wallet,
      'Bridge' AS wallet_type,
      'Optimism' AS protocol
    UNION ALL
    SELECT
      0x12ed69359919fc775bc2674860e8fe2d2b6a7b5d AS wallet,
      'Bridge' AS wallet_type,
      'RSK' AS protocol
    UNION ALL
    SELECT
      0x4aa42145aa6ebf72e164c9bbc74fbd3788045016 AS wallet,
      'Bridge' AS wallet_type,
      'xDAI' AS protocol
    UNION ALL
    SELECT
      0xdac7bb7ce4ff441a235f08408e632fa1d799a147 AS wallet,
      'Bridge' AS wallet_type,
      'Avalanche' AS protocol
    UNION ALL
    SELECT
      0xc564ee9f21ed8a2d8e7e76c085740d5e4c5fafbe AS wallet,
      'Bridge' AS wallet_type,
      'Anyswap: Fantom' AS protocol
    UNION ALL
    SELECT
      0x9a8c4bdcd75cfa1059a6e453ac5ce9d3f5c82a35 AS wallet,
      'Bridge' AS wallet_type,
      'Fantom' AS protocol /* Anyswap */
    UNION ALL
    SELECT
      0x13b432914a996b0a48695df9b2d701eda45ff264 AS wallet,
      'Bridge' AS wallet_type,
      'Nerve' AS protocol
    UNION ALL
    SELECT
      0x23ddd3e3692d1861ed57ede224608875809e127f AS wallet,
      'Bridge' AS wallet_type,
      'Near' AS protocol /* Rainbow bridge */
    UNION ALL
    SELECT
      0x3014ca10b91cb3d0ad85fef7a3cb95bcac9c0f79 AS wallet,
      'Bridge' AS wallet_type,
      'Fuse' AS protocol
    UNION ALL
    SELECT
      0xabea9132b05a70803a4e85094fd0e1800777fbef AS wallet,
      'Bridge' AS wallet_type,
      'zkSync' AS protocol
    UNION ALL
    SELECT
      0xa68d85df56e733a06443306a095646317b5fa633 AS wallet,
      'Bridge' AS wallet_type,
      'Hermez' AS protocol
    UNION ALL
    SELECT
      0x5fdcca53617f4d2b9134b29090c87d01058e27e9 AS wallet,
      'Bridge' AS wallet_type,
      'Immutable X' AS protocol
    UNION ALL
    SELECT
      0x2dccdb493827e15a5dc8f8b72147e6c4a5620857 AS wallet,
      'Bridge' AS wallet_type,
      'Harmony' AS protocol
    UNION ALL
    SELECT
      0x070cb1270a4b2ba53c81cef89d0fd584ed4f430b AS wallet,
      'Bridge' AS wallet_type,
      'OMG Network' AS protocol
    UNION ALL
    SELECT
      0x737901bea3eeb88459df9ef1be8ff3ae1b42a2ba AS wallet,
      'Bridge' AS wallet_type,
      'Aztec' AS protocol
    UNION ALL
    SELECT
      0xE78388b4CE79068e89Bf8aA7f218eF6b9AB0e9d0 AS wallet,
      'Bridge' AS wallet_type,
      'Avalanche' AS protocol
    UNION ALL
    SELECT
      0x1bf68a9d1eaee7826b3593c20a0ca93293cb489a AS wallet,
      'Bridge' AS wallet_type,
      'Orbit Chain' AS protocol
    UNION ALL
    SELECT
      0x5777d92f208679DB4b9778590Fa3CAB3aC9e2168 AS wallet,
      'Dex' AS wallet_type,
      'Uniswap v3' AS protocol
    UNION ALL
    SELECT
      0x97e7d56A0408570bA1a7852De36350f7713906ec AS wallet,
      'Dex' AS wallet_type,
      'Uniswap v3' AS protocol
    UNION ALL
    SELECT
      0x9a315bdf513367c0377fb36545857d12e85813ef AS wallet,
      'Other' AS wallet_type,
      'Olympus Treasury' AS protocol
    UNION ALL
    SELECT
      0x8eb8a3b98659cce290402893d0123abb75e3ab28 AS wallet,
      'Bridge' AS wallet_type,
      'Avalanche' AS protocol
    UNION ALL
    SELECT
      0x8ffae111ab06f532a18418190129373d14570014 AS wallet,
      'Treasury' AS wallet_type,
      'Fei' AS protocol
    UNION ALL
    SELECT
      0x31f8cc382c9898b273eff4e0b7626a6987c846e8 AS wallet,
      'Treasury' AS wallet_type,
      'Olympus' AS protocol
    UNION ALL
    SELECT
      0x27182842e098f60e3d576794a5bffb0777e025d3 AS wallet,
      'Lending' AS wallet_type,
      'Euler' AS protocol
    UNION ALL
    SELECT
      0x66c57bf505a85a74609d2c83e94aabb26d691e1f AS wallet,
      'CeFi' AS wallet_type,
      'Kraken' AS protocol
    UNION ALL
    SELECT
      0xdc1664458d2f0b6090bea60a8793a4e66c2f1c00 AS wallet,
      'Bridge' AS wallet_type,
      'OMG - Boba' AS protocol
  ),
  deltas AS (
    SELECT
      dst AS wallet,
      TRY_CAST("evt_block_time" AS DATE) AS dt,
      CAST(wad AS INT256) AS delta
    FROM
      maker_ethereum.DAI_evt_Transfer
    UNION ALL
    SELECT
      src AS wallet,
      TRY_CAST("evt_block_time" AS DATE) AS dt,
      - CAST(wad AS INT256) AS delta
    FROM
      maker_ethereum.DAI_evt_Transfer
  ), min_dts AS
  (
    SELECT wallet
    , MIN(dt) AS min_dt
    FROM deltas
    LEFT JOIN addresses USING (wallet)
    WHERE wallet <> 0x0000000000000000000000000000000000000000
    AND wallet_type = 'Lending'
    GROUP BY 1
  ), noop_filling /* Generate one 'touch' per lending wallet per month to avoid holes */ AS (
    SELECT wallet
    , dt
    , CAST(NULL AS INT256) AS delta
    FROM min_dts
    CROSS JOIN 
    UNNEST ( SEQUENCE(min_dt, CURRENT_DATE, INTERVAL '1' day) ) AS _u (dt)
  ), deltas2 AS
  (
    SELECT * FROM deltas
    UNION ALL
    SELECT * FROM noop_filling
  ),
  grouped AS (
    SELECT
      COALESCE(protocol, 'Other') AS wallet,
      dt,
      SUM(delta) AS delta
    FROM
      deltas2
      LEFT JOIN addresses USING (wallet)
    WHERE
      wallet <> 0x0000000000000000000000000000000000000000
      AND wallet_type = 'Lending'
    GROUP BY
      1,
      2
  ),
  balances AS (
    SELECT
      wallet,
      dt,
      SUM(delta) OVER (
        PARTITION BY
          wallet
        ORDER BY
          dt
      ) / CAST(POWER(10, 18) AS DOUBLE) AS balance
    FROM
      grouped
  )
SELECT
  wallet,
  TRY_CAST(
    dt AS TIMESTAMP
    WITH
      TIME ZONE
  ) AS dt,
  balance
FROM
  balances
WHERE
  dt > CAST('2020-06-01' AS TIMESTAMP)