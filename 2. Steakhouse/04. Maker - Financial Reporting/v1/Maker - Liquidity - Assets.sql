WITH
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
      CAST(d AS TIMESTAMP) - INTERVAL '1' SECOND AS call_block_time,
      TRY_CAST(NULL AS INT256) AS dart,
      TRY_CAST(NULL AS INT256) AS rate
    FROM
      ilks
      CROSS JOIN UNNEST (
        SEQUENCE(DATE(starting_use) + INTERVAL '1' DAY, CURRENT_DATE + INTERVAL '1' DAY, INTERVAL '1' day)
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
      TRY_CAST(call_block_time AS DATE) AS dt,
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
  )
SELECT
  TRY_CAST(dt AS TIMESTAMP) AS dt,
  SUM(
    CASE
      WHEN collateral LIKE 'PSM%' THEN debt
      WHEN collateral IN (
        'USDC-A',
        'USDC-B',
        'TUSD-A',
        'GUSD-A',
        'PAXUSD-A'
      ) THEN debt
    END
  ) AS assets_stablecoins,
  SUM(
    CASE
      WHEN collateral LIKE 'PSM-USDC%' THEN debt
      WHEN collateral IN ('USDC-A', 'USDC-B') THEN debt
    END
  ) AS assets_usdc,
  SUM(debt) AS assets_size,
  SUM(
    CASE
      WHEN collateral LIKE 'PSM%' THEN debt
      WHEN collateral IN (
        'USDC-A',
        'USDC-B',
        'TUSD-A',
        'GUSD-A',
        'PAXUSD-A'
      ) THEN debt
    END
  ) / CAST(SUM(debt) AS DOUBLE) AS stablecoins_ratio,
  SUM(
    CASE
      WHEN collateral LIKE 'PSM-USDC%' THEN debt
      WHEN collateral IN ('USDC-A', 'USDC-B') THEN debt
    END
  ) / CAST(SUM(debt) AS DOUBLE) AS usdc_ratio,
  1.0 AS " "
FROM
  group_by
GROUP BY
  1
ORDER BY
  1 DESC