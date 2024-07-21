WITH
  raw_data AS (
    SELECT
      i AS ilk,
      call_block_time,
      call_block_number,
      dart,
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
      call_block_number,
      dart,
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
      call_block_number,
      NULL AS dart,
      rate
    FROM
      maker_ethereum.VAT_call_fold
    WHERE
      call_success
      AND rate <> CAST(0 AS INT256)
  ),
  running_amounts AS (
    SELECT
      ilk,
      call_block_time,
      call_block_number,
      rate,
      SUM(dart) OVER (
        PARTITION BY
          ilk
        ORDER BY
          call_block_number
      ) AS dart
    FROM
      raw_data
  ),
  debt_revenues AS (
    SELECT
      ilk,
      call_block_time,
      call_block_number,
      dart,
      (dart * rate) / CAST(POWER(10, 45) AS DOUBLE) AS interest
    FROM
      running_amounts
    WHERE
      NOT rate IS NULL
  ),
  revenues_ilk_detail AS (
    SELECT
      DATE_FORMAT(call_block_time, '%Y-%m') AS period,
      REPLACE(FROM_UTF8(ilk),U&'\0000', '') AS collateral,
      SUM(interest) AS revenues
    FROM
      debt_revenues
    GROUP BY
      1,
      2
  ),
  other_cat AS (
    SELECT
      collateral,
      SUM(revenues) AS collateral_total_revenues
    FROM
      revenues_ilk_detail
    GROUP BY
      1
  ),
  other_ca_order AS (
    SELECT
      collateral,
      ROW_NUMBER() OVER (
        ORDER BY
          COALESCE(collateral_total_revenues, 0) DESC
      ) AS collateral_rank
    FROM
      other_cat
    WHERE
      NOT collateral IN (
        'USDC-A',
        'USDC-B',
        'TUSD-A',
        'GUSD-A',
        'PAXUSD-A',
        'PSM-USDC-A'
      )
  )
SELECT
  period,
  CASE
    WHEN collateral_rank > 5 THEN 'Others'
    ELSE collateral
  END AS collateral,
  CASE
    WHEN collateral_rank > 5 THEN 99
    ELSE collateral_rank
  END AS collateral_rank,
  SUM(revenues) AS revenues
FROM
  revenues_ilk_detail
  INNER JOIN other_ca_order USING (collateral)
GROUP BY
  1,
  2,
  3
ORDER BY
  1 DESC