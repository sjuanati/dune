WITH
  assets AS (
    SELECT
      'RWA001-A' AS collateral,
      '6S Capital' AS collateral_name,
      'Real estate' AS collateral_type
    UNION ALL
    SELECT
      'RWA002-A' AS collateral,
      'New Silver' AS collateral_name,
      'Real estate' AS collateral_type
    UNION ALL
    SELECT
      'RWA003-A' AS collateral,
      'ConsolFreight' AS collateral_name,
      'Trade Finance' AS collateral_type
    UNION ALL
    SELECT
      'RWA004-A' AS collateral,
      'Harbor Trade Credit' AS collateral_name,
      'Trade Finance' AS collateral_type
    UNION ALL
    SELECT
      'RWA005-A' AS collateral,
      'FortunaFi' AS collateral_name,
      'Revenue-based Finance' AS collateral_type
    UNION ALL
    SELECT
      'RWA006-A' AS collateral,
      'Peoples Company' AS collateral_name,
      'Farmland' AS collateral_type
    UNION ALL
    SELECT
      'RWA007-A' AS collateral,
      'MIP65' AS collateral_name,
      'Short-term bonds' AS collateral_type
    UNION ALL
    SELECT
      'RWA008-A' AS collateral,
      'OFH - SocGen' AS collateral_name,
      'Bank' AS collateral_type
    UNION ALL
    SELECT
      'RWA009-A' AS collateral,
      'HVBank' AS collateral_name,
      'Bank' AS collateral_type
    UNION ALL
    SELECT
      'RWA015-A' AS collateral,
      'Andromeda' AS collateral_name,
      'Short-term bonds' AS collateral_type
    UNION ALL
    SELECT
      'RWA014-A' AS collateral,
      'Coinbase Custody' AS collateral_name,
      'Custody' AS collateral_type
    UNION ALL
    SELECT
      'RWA013-A' AS collateral,
      'BlockTower' AS collateral_name,
      'Private Credit' AS collateral_type
    UNION ALL
    SELECT
      'RWA012-A' AS collateral,
      'BlockTower' AS collateral_name,
      'Private Credit' AS collateral_type
  ),
  lending_assets_1 AS (
    SELECT
      i AS ilk,
      call_block_time,
      dart AS dart,
      NULL AS rate
    FROM
      maker_ethereum.VAT_call_frob
    WHERE
      call_success
      AND dart <> CAST(0 AS INT256)
    UNION ALL
    SELECT
      i AS ilk,
      call_block_time,
      dart AS dart,
      CAST(0 AS INT256) AS rate
    FROM
      maker_ethereum.VAT_call_grab
    WHERE
      call_success
      AND dart <> CAST(0 AS INT256)
    UNION ALL
    SELECT
      i AS ilk,
      call_block_time,
      NULL AS dart,
      rate AS rate
    FROM
      maker_ethereum.VAT_call_fold
    WHERE
      call_success
      AND rate <> CAST(0 AS INT256)
  ),
  ilks /* Find the first usage of an ilk */ AS (
    SELECT
      ilk,
      MIN(call_block_time) AS starting_use
    FROM
      lending_assets_1
    GROUP BY
      ilk
  ),
  noop_filling /* Generate one 'touch' per ilk per month to avoid holes */ AS (
    SELECT
      ilk,
      d AS call_block_time,
      TRY_CAST(NULL AS INT256) AS dart,
      TRY_CAST(NULL AS INT256) AS rate
    FROM
      ilks
      CROSS JOIN UNNEST (
        SEQUENCE(
          TRY_CAST(starting_use AS DATE),
          CURRENT_DATE,
          INTERVAL '1' day
        )
      ) AS _u (d)
  ),
  lending_assets_1_with_filling AS (
    SELECT
      *
    FROM
      lending_assets_1
    UNION ALL
    SELECT
      *
    FROM
      noop_filling
  ),
  lending_assets_2 AS (
    SELECT
      ilk,
      call_block_time,
      COALESCE(
        1 + SUM(rate) OVER (
          PARTITION BY
            ilk
          ORDER BY
            call_block_time
        ) / CAST(POWER(10, 27) AS DOUBLE),
        1
      ) AS rate,
      SUM(dart) OVER (
        PARTITION BY
          ilk
        ORDER BY
          call_block_time
      ) / CAST(POWER(10, 18) AS DOUBLE) AS dart
    FROM
      lending_assets_1_with_filling
  ),
  with_rk AS (
    SELECT
      TRY_CAST(call_block_time AS DATE) AS date,
      REPLACE(FROM_UTF8(ilk),U&'\0000', '') AS collateral,
      dart * rate AS debt,
      ROW_NUMBER() OVER (
        PARTITION BY
          ilk,
          TRY_CAST(call_block_time AS DATE)
        ORDER BY
          call_block_time DESC
      ) AS rk
    FROM
      lending_assets_2
  ),
  group_by AS (
    SELECT
      *
    FROM
      with_rk
    WHERE
      rk = 1
      AND debt <> 0.0
      AND collateral LIKE 'RWA%'
  )
SELECT
  collateral,
  collateral_name,
  collateral_type,
  TRY_CAST(date AS TIMESTAMP) AS date,
  debt
FROM
  group_by
  LEFT JOIN assets USING (collateral)
WHERE
  date = CURRENT_DATE
ORDER BY
  date DESC,
  collateral_name