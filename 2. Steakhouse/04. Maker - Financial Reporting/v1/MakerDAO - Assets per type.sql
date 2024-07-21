WITH
  lending_assets_1 AS (
    SELECT
      i AS ilk,
      call_block_time,
      call_trace_address,
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
      call_trace_address,
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
      call_trace_address,
      NULL AS dart,
      rate AS rate
    FROM
      maker_ethereum.VAT_call_fold
    WHERE
      call_success
      AND rate <> CAST(0 AS INT256)
  ),
  gusd_settings AS (
    SELECT
      TRY_CAST(period AS DATE) AS dt,
      case when period < date'2013-07-01' then 0.0125
        else 0.028 -- https://forum.makerdao.com/t/gusd-makerdao-partnership-update-july-23/21401
        end aS rate,
      100 * POWER(10, 6) AS min_volume
    FROM
      UNNEST (
        SEQUENCE(
          TRY_CAST('2022-10-25' AS DATE),
          CURRENT_DATE,
          INTERVAL '1' day
        )
      ) AS _u (period)
  ),
  mip65_settings AS (
    SELECT
      TRY_CAST(period AS DATE) AS dt,
      CASE
        WHEN period >= CAST('2023-07-01' AS TIMESTAMP) THEN 0.045
        WHEN period >= CAST('2023-01-01' AS TIMESTAMP) THEN 0.04
        WHEN period >= CAST('2022-10-27' AS TIMESTAMP) THEN 0.04
        WHEN period >= CAST('2022-10-13' AS TIMESTAMP) THEN 0.03
        ELSE 0.0
      END AS rate
    FROM
      UNNEST (
        SEQUENCE(
          TRY_CAST('2022-10-13' AS DATE),
          CURRENT_DATE,
          INTERVAL '1' day
        )
      ) AS _u (period)
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
      CAST(NULL AS ARRAY(BIGINT)) AS call_trace_address,
      TRY_CAST(NULL AS INT256) AS dart,
      TRY_CAST(NULL AS INT256) AS rate,
      TRY_CAST(NULL AS DECIMAL) AS sf
    FROM
      ilks
      CROSS JOIN UNNEST (
        SEQUENCE(DATE(starting_use) + INTERVAL '1' DAY, CURRENT_DATE + INTERVAL '1' DAY, INTERVAL '1' day)
      ) AS _u (d)
  ),
  rates AS (
    SELECT
      call_block_time,
      ilk,
      POWER((data_uint256 / POWER(10, 27)), (3600 * 24 * 365)) - 1 AS sf
    FROM
      maker_ethereum.JUG_call_file
    WHERE
      call_success
      AND ilk <> 0x4449524543542d535041524b2d44414900000000000000000000000000000000 -- DIRECT-SPARK-DAI
    UNION ALL
    SELECT
        period,
        0x4449524543542d535041524b2d44414900000000000000000000000000000000 as ilk, -- DIRECT-SPARK-DAI
        supply_rate as rate
    FROM dune.steakhouse.result_lending_markets
    WHERE blockchain = 'ethereum'
    AND protocol = 'spark'
    AND version = '1'
    AND symbol = 'DAI'
  ),
  lending_assets_1_with_filling AS (
    SELECT
      *,
      TRY_CAST(NULL AS DECIMAL) AS sf
    FROM
      lending_assets_1
    UNION ALL
    SELECT
      *
    FROM
      noop_filling
    UNION ALL
    SELECT
      ilk,
      call_block_time,
      CAST(NULL AS ARRAY(BIGINT)) AS call_trace_address,
      TRY_CAST(NULL AS INT256) AS dart,
      TRY_CAST(NULL AS INT256) AS rate,
      sf
    FROM
      rates
  ),
  lending_assets_2 AS (
    SELECT
      ilk,
      call_block_time,
      rate AS r,
      COALESCE(
        1 + SUM(rate) OVER (
          PARTITION BY
            ilk
          ORDER BY
            call_block_time, call_trace_address
        ) / POWER(10, 27),
        1
      ) AS rate,
      SUM(dart) OVER (
        PARTITION BY
          ilk
        ORDER BY
          call_block_time, call_trace_address
      ) / POWER(10, 18) AS dart,
      SUM(
        CASE
          WHEN NOT sf IS NULL THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          ilk
        ORDER BY
          call_block_time
      ) AS sf_grp,
      sf
    FROM
      lending_assets_1_with_filling
  ),
  with_rk AS (
    SELECT
      TRY_CAST(call_block_time AS DATE) AS dt,
      REPLACE(FROM_UTF8(ilk),U&'\0000', '') AS collateral,
      dart * rate AS debt,
      dart * (r / POWER(10, 27)) AS revenues,
      MAX(sf) OVER (
        PARTITION BY
          ilk,
          sf_grp
      ) AS sf,
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
    SELECT *
    FROM 
    (
        SELECT
          *,
          sf AS rate,
          debt * sf AS annual_revenues,
          SUM(revenues) OVER (
            PARTITION BY
              collateral,
              dt
          ) AS rev
        FROM
          with_rk
    )
    WHERE
      rk = 1
      AND debt <> 0.0
  ),
  d3m AS (
    SELECT
      dt,
      collateral,
      debt,
      COALESCE(daily_revenue, 0) * 365 AS annual_revenues,
      COALESCE(daily_revenue, 0) AS rev
    FROM
      group_by
      LEFT JOIN (SELECT * FROM query_2611857) sub USING (dt)
    WHERE
      collateral = 'DIRECT-AAVEV2-DAI'
    UNION ALL
    SELECT
      dt,
      collateral,
      debt,
      COALESCE(daily_revenue, 0) * 365 AS annual_revenues,
      COALESCE(daily_revenue, 0) AS rev
    FROM
      group_by
      LEFT JOIN (SELECT * FROM query_2617747) sub USING (dt)
    WHERE
      collateral = 'DIRECT-COMPV2-DAI'
    UNION ALL
    SELECT
      dt,
      collateral,
      debt,
      annual_revenues,
      rev
    FROM
      group_by
    WHERE
      collateral NOT IN ('DIRECT-AAVEV2-DAI', 'DIRECT-COMPV2-DAI')
  ),
  group_by_cat AS (
    SELECT
      dt,
      CASE
        WHEN collateral LIKE 'PSM%' THEN 'Stablecoins'
        WHEN collateral IN (
          'USDC-A',
          'USDC-B',
          'USDT-A',
          'TUSD-A',
          'GUSD-A',
          'PAXUSD-A'
        ) THEN 'Stablecoins'
        WHEN collateral LIKE 'ETH-%' THEN 'ETH'
        WHEN collateral LIKE 'WSTETH-%' THEN 'ETH'
        WHEN collateral LIKE 'WBTC-%' THEN 'WBTC'
        WHEN collateral LIKE 'UNIV2%' THEN 'Liquidity Pools'
        WHEN collateral LIKE 'GUNI%' THEN 'Liquidity Pools'
        WHEN collateral LIKE 'RWA015-A' THEN 'TBills'
        WHEN collateral LIKE 'RWA007-A' THEN 'TBills'
        WHEN collateral LIKE 'RWA014-A' THEN 'Coinbase'
        WHEN collateral LIKE 'RWA%' THEN 'RWA'
        WHEN collateral LIKE 'DIRECT%' THEN 'Lending Protocols'
        ELSE 'Others'
      END AS collateral,
      debt AS asset,
      rev AS revenues,
      CASE
        WHEN collateral = 'PSM-GUSD-A'
        AND debt > gusd.min_volume THEN debt * gusd.rate
        WHEN collateral = 'RWA007-A' THEN debt * mip65.rate
        WHEN collateral = 'RWA009-A' THEN debt * mip65.rate
        WHEN collateral = 'RWA015-A' THEN debt * mip65.rate
        WHEN collateral = 'RWA014-A' THEN debt * 0.0025 -- approximated http://forum.makerdao.com/t/mip81-coinbase-usdc-institutional-rewards/17703/254?u=sebventures
        ELSE annual_revenues
      END AS annual_revenues
    FROM
      d3m
      LEFT JOIN gusd_settings AS gusd USING (dt)
      LEFT JOIN mip65_settings AS mip65 USING (dt)
  ),
  group_by_dt_cat AS (
    SELECT
      TRY_CAST(
        dt AS TIMESTAMP
        WITH
          TIME ZONE
      ) AS dt,
      collateral,
      SUM(asset) AS asset,
      SUM(annual_revenues) AS annual_revenues,
      SUM(annual_revenues) / SUM(asset) AS blended_rate,
      SUM(revenues) AS revenues
    FROM
      group_by_cat
    GROUP BY
      1,
      2
  )
SELECT
  *,
  SUM(annual_revenues) OVER (
    PARTITION BY
      dt
  ) AS total_annual_revenues,
  SUM(asset) OVER (
    PARTITION BY
      dt
  ) AS total_asset,
  SUM(annual_revenues) OVER (
    PARTITION BY
      dt
  ) / SUM(asset) OVER (
    PARTITION BY
      dt
  ) AS total_blended_rate
FROM
  group_by_dt_cat
WHERE
  dt > CAST('2020-01-01' AS TIMESTAMP)

ORDER BY
  1 DESC,
  2