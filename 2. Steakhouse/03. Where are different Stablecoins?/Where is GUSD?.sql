-- This query has been migrated to run on DuneSQL.
-- If you need to change back because you’re seeing issues, use the Query History view to restore the previous version.
-- If you notice a regression, please email dunesql-feedback@dune.com.

WITH
  addresses AS (
    with address_list(wallet, wallet_type, protocol) as (
    
        VALUES
        (0x5f65f7b609678448494de4c87521cdf6cef1e932, 'Gemini', 'Gemini'),
        (0x9C2eA4689aaDC0d3B85F53eEE250C0A197fbbe54, 'Gemini', 'From Gemini: Deployer 2'),
        (0x8d6f396d210d385033b348bcae9e4f9ea4e045bd, 'Gemini', 'From Gemini: Deployer 2'),
        (0x26994D7c461a91Ef5324f7058C10b18D9DD8D43A, 'CeFi', 'BlockFi'),
        (0x4f062658eaaf2c1ccf8c8e36d6824cdf41167956, 'DEX', 'Curve'),
        (0x79a0fa989fb7adf1f8e80c93ee605ebb94f7c6a5, 'PSM', 'PSM'),
        (0x4e43151b78b5fbb16298c1161fcbf7531d5f8d93, 'DEX', 'Curve'),
        (0xd37ee7e4f452c6638c96536e68090de8cbcdb583, 'Lending', 'Aave'),
        (0x6262998ced04146fa42253a5c0af90ca02dfd2a3, 'CeFi', 'Crypto.com'),
        (0x46340b20830761efd32832a74d7169b29feb9758, 'CeFi', 'Crypto.com'),
        (0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43, 'CeFi', 'Coinbase'),
        (0x6cc5f688a315f3dc28a7781717a9a798a59fda7b, 'CeFi', 'OKEx'),
        (0x5aa1356999821b533ec5d9f79c23b8cb7c295c61, 'DEX', 'Uniswap v3'),
        (0xe5859f4efc09027a9b718781dcb2c6910cac6e91, 'Other', 'Smoothy.finance'),
        (0x075e72a5edf65f0a5f44699c7654c1a76941ddc8, 'Other', 'PulseX: Sacrifice'),
        (0x22FFDA6813f4F34C520bf36E5Ea01167bC9DF159, 'CeFi', 'BlockFi'),
        (0xC131701Ea649AFc0BfCc085dc13304Dc0153dc2e, 'CeFi', 'Celsius Network'),
        (0xD37BbE5744D730a1d98d8DC97c42F0Ca46aD7146, 'DEX', 'Thorchain Router'),
        (0x9281035DF6F00557c0285d7df21d323C2E2f99aD, 'CeFi', 'Huobi')
      )
      SELECT wallet, wallet_type, protocol
      FROM address_list

  ),
  deltas AS (
    SELECT
      "to" AS wallet,
      TRY_CAST("evt_block_time" AS DATE) AS dt,
      cast("value" as int256) AS delta
    FROM
      erc20_ethereum.evt_Transfer
    WHERE
      contract_address = 0x056fd409e1d7a124bd7017459dfea2f387b6d5cd
    UNION ALL
    SELECT
      "from" AS wallet,
      TRY_CAST("evt_block_time" AS DATE) AS dt,
      - cast("value" as int256) AS delta
    FROM
      erc20_ethereum.evt_Transfer
    WHERE
      contract_address = 0x056fd409e1d7a124bd7017459dfea2f387b6d5cd
    UNION ALL
    SELECT
      wallet,
      dt,
      cast(0 as int256) AS delta
    FROM
      addresses
      CROSS JOIN UNNEST (
        SEQUENCE(
          CAST('2020-01-01' AS TIMESTAMP),
          CAST(CURRENT_DATE AS TIMESTAMP),
          INTERVAL '1' day
        )
      ) AS _u (dt)
  ),
  contracts /* Fix duplicates in the contract table */ AS (
    SELECT
      address,
      1 AS id
    FROM
      ethereum."contracts"
    GROUP BY
      1
  ),
  grouped AS (
    SELECT
      COALESCE(
        CASE
          WHEN wallet_type IS NULL
          AND c.id IS NULL THEN 'EOA'
          ELSE wallet_type
        END,
        'Other'
      ) AS wallet,
      dt,
      SUM(delta) AS delta
    FROM
      deltas
      LEFT JOIN addresses USING (wallet)
      LEFT JOIN contracts AS c ON wallet = address
    WHERE
      wallet <> 0x0000000000000000000000000000000000000000
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
      ) / CAST(POWER(10, 2) AS DOUBLE) AS balance
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
  balance,
  SUM(balance) OVER (
    PARTITION BY
      dt
  ) AS total_balance,
  SUM(
    CASE
      WHEN wallet <> 'Gemini' THEN balance
    END
  ) OVER (
    PARTITION BY
      dt
  ) AS total_balance_non_gemini
FROM
  balances
WHERE
  dt > CAST('2020-06-01' AS TIMESTAMP)
ORDER BY
  dt DESC NULLS FIRST
