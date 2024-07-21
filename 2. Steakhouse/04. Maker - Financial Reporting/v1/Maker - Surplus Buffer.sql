WITH
  periods AS (
    SELECT
      TRY_CAST(period AS DATE) AS period
    FROM
      UNNEST (
        SEQUENCE(
          TRY_CAST('2019-11-01' AS DATE),
          CURRENT_DATE,
          INTERVAL '1' day
        )
      ) AS _u (period)
  ),
  maker_addresses AS (
    SELECT
      0xa950524441892a31ebddf91d3ceefa04bf454466 AS address,
      'Vow' AS name
  ),
  sb_dai_in AS (
    SELECT
      TRY_CAST(call_block_time AS DATE) AS period,
      SUM(rad / CAST(POWER(10, 45) AS DOUBLE)) AS dai_inflow
    FROM
      maker_ethereum.VAT_call_move
    WHERE
      dst IN (
        SELECT
          address
        FROM
          maker_addresses
      )
      AND call_success
    GROUP BY
      1
  ),
  sb_dai_out AS (
    SELECT
      TRY_CAST(call_block_time AS DATE) AS period,
      SUM(rad / CAST(POWER(10, 45) AS DOUBLE)) AS dai_outflow
    FROM
      maker_ethereum.VAT_call_move
    WHERE
      src IN (
        SELECT
          address
        FROM
          maker_addresses
      )
      AND call_success
    GROUP BY
      1
  ),
  sb_sin_out AS (
    SELECT
      TRY_CAST(call_block_time AS DATE) AS period,
      SUM(rad / CAST(POWER(10, 45) AS DOUBLE)) AS sin_outflow
    FROM
      maker_ethereum.VAT_call_suck
    WHERE
      u IN (
        SELECT
          address
        FROM
          maker_addresses
      )
      AND call_success
    GROUP BY
      1
  ),
  sb_sin_in AS (
    SELECT
      TRY_CAST(call_block_time AS DATE) AS period,
      SUM(rad / CAST(POWER(10, 45) AS DOUBLE)) AS sin_inflow
    FROM
      maker_ethereum.VAT_call_suck
    WHERE
      v IN (
        SELECT
          address
        FROM
          maker_addresses
      )
      AND call_success
    GROUP BY
      1
  ),
  sb_fess AS (
    SELECT
      TRY_CAST(call_block_time AS DATE) AS period,
      SUM(tab / CAST(POWER(10, 45) AS DOUBLE)) AS fess
    FROM
      maker_ethereum.VOW_call_fess
    WHERE
      call_success
    GROUP BY
      1
  ),
  sb_accrued_interest_1 AS (
    SELECT
      i AS ilk,
      call_block_time,
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
      call_block_time - INTERVAL '5' second,
      NULL AS dart,
      rate
    FROM
      maker_ethereum.VAT_call_fold
    WHERE
      call_success
      AND rate <> CAST(0 AS INT256)
  ),
  sb_accrued_interest_2 AS (
    SELECT
      *,
      SUM(dart) OVER (
        PARTITION BY
          ilk
        ORDER BY
          call_block_time
      ) AS debt
    FROM
      sb_accrued_interest_1
  ),
  sb_accrued_interest_3 AS (
    SELECT
      REPLACE(FROM_UTF8(ilk),U&'\0000', '') AS ilk,
      TRY_CAST(call_block_time AS DATE) AS period,
      SUM(debt * rate) / CAST(POWER(10, 45) AS DOUBLE) AS lending_revenues
    FROM
      sb_accrued_interest_2
    WHERE
      NOT rate IS NULL
    GROUP BY
      1,
      2
  ),
  sb_accrued_interest AS (
    SELECT
      period,
      SUM(lending_revenues) AS accrued_interests
    FROM
      sb_accrued_interest_3
    GROUP BY
      1
  ),
  sb_fusion AS (
    SELECT
      period,
      SUM(dai_inflow) AS dai_inflow,
      SUM(dai_outflow) AS dai_outflow,
      SUM(sin_outflow) AS sin_outflow,
      SUM(sin_inflow) AS sin_inflow,
      SUM(fess) AS fess,
      SUM(accrued_interests) AS accrued_interests
    FROM
      periods
      LEFT JOIN sb_dai_in USING (period)
      LEFT JOIN sb_dai_out USING (period)
      LEFT JOIN sb_sin_out USING (period)
      LEFT JOIN sb_sin_in USING (period)
      LEFT JOIN sb_fess USING (period)
      LEFT JOIN sb_accrued_interest USING (period)
    GROUP BY
      1
  ),
  sb AS (
    SELECT
      TRY_CAST(
        period AS TIMESTAMP
        WITH
          TIME ZONE
      ) AS period,
      SUM(
        COALESCE(dai_inflow, 0) - COALESCE(dai_outflow, 0) - COALESCE(sin_outflow, 0) + COALESCE(sin_inflow, 0) - COALESCE(fess, 0) + COALESCE(accrued_interests, 0)
      ) OVER (
        ORDER BY
          period
      ) AS surplus_buffer
    FROM
      sb_fusion
  ),
  sb2 AS (
    SELECT
      *,
      (
        surplus_buffer - LAG(surplus_buffer, 30) OVER (
          ORDER BY
            period
        )
      ) / CAST(30 AS DOUBLE) * 365 AS delta_30d,
      (
        surplus_buffer - LAG(surplus_buffer, 90) OVER (
          ORDER BY
            period
        )
      ) / CAST(90 AS DOUBLE) * 365 AS delta_90d
    FROM
      sb
  )
SELECT
  *
FROM
  sb2
WHERE
  period > CURRENT_DATE - INTERVAL '365' DAY
ORDER BY
  1 DESC