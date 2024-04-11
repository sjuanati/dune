WITH
  addresses AS (
    SELECT
      0x99C9fc46f92E8a1c0deC1b1747d010903E884bE1 AS wallet,
      'Bridge' AS wallet_type,
      'Optimism' AS protocol
    UNION ALL
    SELECT
      0x467194771dAe2967Aef3ECbEDD3Bf9a310C76C65 AS wallet,
      'Bridge' AS wallet_type,
      'Optimism' AS protocol
    UNION ALL
    SELECT
      0xA10c7CE4b876998858b1a9E12b10092229539400 AS wallet,
      'Bridge' AS wallet_type,
      'Arbitrum' AS protocol
    UNION ALL
    SELECT
      0xa3A7B6F88361F48403514059F1F16C8E78d60EeC AS wallet,
      'Bridge' AS wallet_type,
      'Arbitrum' AS protocol
    UNION ALL
    SELECT
      0xcEe284F754E854890e311e3280b767F80797180d AS wallet,
      'Bridge' AS wallet_type,
      'Arbitrum' AS protocol
    UNION ALL
    SELECT
      0x40ec5b33f54e0e8a33a975908c5ba1c14e5bbbdf AS wallet,
      'Bridge2' AS wallet_type,
      'Polygon' AS protocol
    UNION ALL
    SELECT
      0xabea9132b05a70803a4e85094fd0e1800777fbef AS wallet,
      'Bridge2' AS wallet_type,
      'zkSync' AS protocol
    UNION ALL
    SELECT
      0xa68d85df56e733a06443306a095646317b5fa633 AS wallet,
      'Bridge2' AS wallet_type,
      'Hermez' AS protocol
    UNION ALL
    SELECT
      0x737901bea3eeb88459df9ef1be8ff3ae1b42a2ba AS wallet,
      'Bridge2' AS wallet_type,
      'Aztec' AS protocol
  ),
  deltas AS (
    SELECT
      symbol AS name,
      COALESCE(protocol, 'Other') AS wallet,
      TRY_CAST("evt_block_time" AS DATE) AS dt,
      value / (POWER(10, decimals)) AS delta
    FROM
      erc20_ethereum.evt_Transfer
      INNER JOIN tokens_ethereum.stablecoins USING (contract_address)
      INNER JOIN addresses ON "to" = wallet
    WHERE
      wallet <> 0x0000000000000000000000000000000000000000
      AND wallet_type = 'Bridge'
    UNION ALL
    SELECT
      symbol AS name,
      COALESCE(protocol, 'Other') AS wallet,
      TRY_CAST("evt_block_time" AS DATE) AS dt,
      - CAST(value AS INT256) / (POWER(10, decimals)) AS delta
    FROM
      erc20_ethereum.evt_Transfer
      INNER JOIN tokens_ethereum.stablecoins USING (contract_address)
      INNER JOIN addresses ON "from" = wallet
    WHERE
      wallet <> 0x0000000000000000000000000000000000000000
      AND wallet_type = 'Bridge'
    UNION ALL
    SELECT
      symbol AS name,
      COALESCE(protocol, 'Other') AS wallet,
      dt,
      0 AS delta
    FROM
      UNNEST (
        SEQUENCE(
          TRY_CAST('2021-01-01' AS DATE),
          CURRENT_DATE,
          INTERVAL '1' day
        )
      ) AS _u (dt)
      CROSS JOIN tokens_ethereum.stablecoins
      CROSS JOIN addresses
    WHERE
      wallet <> 0x0000000000000000000000000000000000000000
      AND wallet_type = 'Bridge'
  ),
  grouped AS (
    SELECT
      wallet,
      name,
      dt,
      SUM(delta) AS delta
    FROM
      deltas
    GROUP BY
      1,
      2,
      3
  ),
  balances AS (
    SELECT
      wallet,
      name,
      dt,
      SUM(delta) OVER (
        PARTITION BY
          wallet,
          name
        ORDER BY
          dt
      ) AS balance,
      MAX(delta) OVER (
        PARTITION BY
          name
      ) AS active
    FROM
      grouped
  )
SELECT
  wallet,
  name,
  TRY_CAST(
    dt AS TIMESTAMP
    WITH
      TIME ZONE
  ) AS dt,
  balance
FROM
  balances
WHERE
  dt > CAST('2021-06-01' AS TIMESTAMP)
  AND active > 0
  AND wallet = 'Optimism'