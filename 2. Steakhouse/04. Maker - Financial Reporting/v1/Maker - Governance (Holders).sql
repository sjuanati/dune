-- This query has been migrated to run on DuneSQL. If you need to change back because you’re seeing issues, use the Query History view to restore the previous version. If you notice a regression, please email dunesql-feedback@dune.com.

WITH
  persons AS (
    SELECT
      TRY_CAST(
        0x05e793ce0c6027323ac150f6d45c2344d28b6019 AS VARBINARY
      ) AS address,
      'a16z' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0x59cee32f3ffaeabc0d4991ac7569ebad09e1a7d4 AS VARBINARY
      ) AS address,
      'Parafi' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0x58f5f0684c381fcfc203d77b2bba468ebb29b098 AS VARBINARY
      ) AS address,
      'BlockTower' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0x0179437cc2f6c3812f48f9376b708c481a725e3f AS VARBINARY
      ) AS address,
      'Standard Crypto' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0x15232c8ceda3293ed5984d4631d4ad95ec39db19 AS VARBINARY
      ) AS address,
      'Standard Crypto' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0x60ffdd5f4735e13c9bc805c4d26f76b0952c2000 AS VARBINARY
      ) AS address,
      'Standard Crypto' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0x979423ab7b42161c31e6c303a4850a454716ce86 AS VARBINARY
      ) AS address,
      'Standard Crypto' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0x9ee6fce533e27a546e07b3b93c7952da00a87d5e AS VARBINARY
      ) AS address,
      'Standard Crypto' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0xf88790fb8f155924a2b3519871711f776a3cd0ff AS VARBINARY
      ) AS address,
      'Standard Crypto' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0xf65475e74c1ed6d004d5240b06e3088724dfda5d AS VARBINARY
      ) AS address,
      'Big Fish' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0x74971f1be0afd1bb820668abfe411d164f17b53c AS VARBINARY
      ) AS address,
      'Big Fish' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0x26732399F47e00739D2b4b0451aCC3F93F7e3a14 AS VARBINARY
      ) AS address,
      'Big Fish' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0xDC306882831243D1E914236464D66cA469093841 AS VARBINARY
      ) AS address,
      'Big Fish' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0xd48d3462C5e5A5d568c8F8ec3366241ed8b46BD1 AS VARBINARY
      ) AS address,
      'Big Fish' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0x563BFb3cC3089cA738C55AB49eB51CB4DDF1DB61 AS VARBINARY
      ) AS address,
      'Big Fish' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0x8b0841A9d098345BfF8f5162a22553113028999a AS VARBINARY
      ) AS address,
      'Big Fish' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0x4c0B1559F1696B47BE69E37Cb897f3832329ae7A AS VARBINARY
      ) AS address,
      'Big Fish' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0xbD27b877D64262A1626f7D3283806B2a8F65Fb11 AS VARBINARY
      ) AS address,
      'Big Fish' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0xD9B012a168Fb6C1B71c24db8CEe1A256b3CAA2A2 AS VARBINARY
      ) AS address,
      'Big Fish' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0x422eac31914B6BDEEF37EEB5e6B35481485a0808 AS VARBINARY
      ) AS address,
      'Big Fish' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0x56A176aCE5516B0F8525b292Ba697A16d5e8a7eb AS VARBINARY
      ) AS address,
      'Old Fish' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0x1eaD7050c94C8A1f08071ddBb28b01b3eB1B3D38 AS VARBINARY
      ) AS address,
      'Old Fish' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0x6b7Ac46d09d2ADF4CeBe2995EbF9d97E13E9E257 AS VARBINARY
      ) AS address,
      'Old Fish' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0x9eF05f7F6deB616fd37aC3c959a2dDD25A54E4F5 AS VARBINARY
      ) AS address,
      'Old Fish' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0xca8d6f69d8f32516a109df68b623452cc9f5e64d AS VARBINARY
      ) AS address,
      'Correlation Fish' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0x11577a8a5baf1e25b9a2d89f39670f447d75c3cd AS VARBINARY
      ) AS address,
      'Correlation Fish' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0xf37216a8ac034d08b4663108d7532dfcb44583ed AS VARBINARY
      ) AS address,
      'Correlation Fish' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0xefcc3401739427eb0491cc27c7baa06817c7dfdb AS VARBINARY
      ) AS address,
      'Shadow 0xefcc' AS name
    UNION ALL
    SELECT
      TRY_CAST(
        0x58f5f0684c381fcfc203d77b2bba468ebb29b098 AS VARBINARY
      ) AS address,
      'BlockTower' AS name
  ),
  delegates AS (
    SELECT
      TRY_CAST(
        0xafaff1a605c373b43727136c995d21a7fcd08989 AS VARBINARY
      ) AS address,
      'Hasu' AS name,
      0 AS legacy
    UNION ALL
    SELECT
      TRY_CAST(
        0x4d3ac33ab1dd7b0f352b8e590fe8b62c4c39ead5 AS VARBINARY
      ) AS address,
      'ACREinvest' AS name,
      0 AS legacy
    UNION ALL
    SELECT
      TRY_CAST(
        0x84b05b0a30b6ae620f393d1037f217e607ad1b96 AS VARBINARY
      ) AS address,
      'Flipside Crypto' AS name,
      0 AS legacy
    UNION ALL
    SELECT
      TRY_CAST(
        0x8804d391472126da56b9a560aef6c6d5aaa7607b AS VARBINARY
      ) AS address,
      'Doo' AS name,
      0 AS legacy
    UNION ALL
    SELECT
      TRY_CAST(
        0xb21e535fb349e4ef0520318acfe589e174b0126b AS VARBINARY
      ) AS address,
      'UltraSchuppi' AS name,
      1 AS legacy
    UNION ALL
    SELECT
      TRY_CAST(
        0xB4b82978FCe6d26A22deA7E653Bb9ce8e14f8056 AS VARBINARY
      ) AS address,
      'UltraSchuppi' AS name,
      0 AS legacy
    UNION ALL
    SELECT
      TRY_CAST(
        0xad2fda5f6ce305d2ced380fdfa791b6a26e7f281 AS VARBINARY
      ) AS address,
      'PaperImperium' AS name,
      0 AS legacy
    UNION ALL
    SELECT
      TRY_CAST(
        0x00daec2c2a6a3fcc66b02e38b7e56dcdfa9347a1 AS VARBINARY
      ) AS address,
      'Planet X' AS name,
      0 AS legacy
    UNION ALL
    SELECT
      TRY_CAST(
        0x845b36e1e4f41a361dd711bda8ea239bf191fe95 AS VARBINARY
      ) AS address,
      'Feedblack Loops' AS name,
      1 AS legacy
    UNION ALL
    SELECT
      TRY_CAST(
        0x92e1Ca8b69A44bB17aFA92838dA68Fc41f12250a AS VARBINARY
      ) AS address,
      'Feedblack Loops' AS name,
      0 AS legacy
    UNION ALL
    SELECT
      TRY_CAST(
        0x0f4Be9f208C552A6b04d9A1222F385785f95beAA AS VARBINARY
      ) AS address,
      'ElPro' AS name,
      0 AS legacy
    UNION ALL
    SELECT
      TRY_CAST(
        0x688d508f3a6b0a377e266405a1583b3316f9a2b3 AS VARBINARY
      ) AS address,
      'ElPro' AS name,
      1 AS legacy
    UNION ALL
    SELECT
      TRY_CAST(
        0x45127ec92b58c3a89e89f63553073adcaf2f1f5f AS VARBINARY
      ) AS address,
      'MonetSupply' AS name,
      0 AS legacy
    UNION ALL
    SELECT
      TRY_CAST(
        0x22d5294a23d49294bf11d9db8beda36e104ad9b3 AS VARBINARY
      ) AS address,
      'MakerMan' AS name,
      0 AS legacy
    UNION ALL
    SELECT
      TRY_CAST(
        0xB0B829a6AaE0F7e59B43391b2C8a1CFD0C801c8C AS VARBINARY
      ) AS address,
      'Gauntlet' AS name,
      1 AS legacy
    UNION ALL
    SELECT
      TRY_CAST(
        0xa149694b5b67e2078576a6f225de6b138efba043 AS VARBINARY
      ) AS address,
      'Gauntlet' AS name,
      0 AS legacy
    UNION ALL
    SELECT
      TRY_CAST(
        0xf60D7a62C98F65480725255e831DE531EFe3fe14 AS VARBINARY
      ) AS address,
      'GFX Labs' AS name,
      0 AS legacy
    UNION ALL
    SELECT
      TRY_CAST(
        0xCdB792c14391F7115Ba77A7Cd27f724fC9eA2091 AS VARBINARY
      ) AS address,
      'Justin Case' AS name,
      0 AS legacy
    UNION ALL
    SELECT
      TRY_CAST(
        0x2C511D932C5a6fE4071262D49bfc018cfBaAa1F5 AS VARBINARY
      ) AS address,
      'Chris Blec' AS name,
      0 AS legacy
    UNION ALL
    SELECT
      TRY_CAST(
        0x4E314eBA76C3062140AD196e4fFd34485e33c5F5 AS VARBINARY
      ) AS address,
      'Llama' AS name,
      0 AS legacy
    UNION ALL
    SELECT
      TRY_CAST(
        0x797D63cB6709c79b9eCA99d9585eA613DA205156 AS VARBINARY
      ) AS address,
      'ChicagoDAO' AS name,
      0 AS legacy
    UNION ALL
    SELECT
      TRY_CAST(
        0x7Ddb50A5b15AeA7e7cf9aC8E55a7F9fd9d05ecc6 AS VARBINARY
      ) AS address,
      'Penn Blockchain' AS name,
      0 AS legacy
    UNION ALL
    SELECT
      TRY_CAST(
        0xF1792852BF860b4ef84a2869DF1550BC80eC0aB7 AS VARBINARY
      ) AS address,
      'London Business School' AS name,
      0 AS legacy
    UNION ALL
    SELECT
      TRY_CAST(
        0xaa19f47e6aCb02dF88efA9f023F2A38412069902 AS VARBINARY
      ) AS address,
      'Mhonkasalo & Teemulau' AS name,
      0 AS legacy
    UNION ALL
    SELECT
      TRY_CAST(
        0xb8df77c3bd57761bd0c55d2f873d3aa89b3da8b7 AS VARBINARY
      ) AS address,
      'Blockchain@Columbia' AS name,
      0 AS legacy
  ),
  stackers AS (
    SELECT
      "from" AS stacker,
      "evt_block_time" AS dt,
      CAST(value AS DOUBLE) / POWER(10, 18) AS power
    FROM
      erc20_ethereum.evt_Transfer
    WHERE
      contract_address = 0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2
      AND "to" = 0x0a3f6849f78076aefadf113f5bed87720274ddc0
    UNION ALL
    SELECT
      "to" AS stacker,
      "evt_block_time" AS dt,
      - CAST(value AS DOUBLE) / POWER(10, 18) AS power
    FROM
      erc20_ethereum.evt_Transfer
    WHERE
      contract_address = 0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2
      AND "from" = 0x0a3f6849f78076aefadf113f5bed87720274ddc0
  ),
  delegates_power AS (
    SELECT
      "to" AS delegate,
      "from" AS delegator,
      "evt_block_time" AS dt,
      CAST(value AS DOUBLE) / POWER(10, 18) AS power
    FROM
      erc20_ethereum.evt_Transfer
      INNER JOIN delegates ON "to" = address
    WHERE
      contract_address = 0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2
      AND "from" <> 0x0a3f6849f78076aefadf113f5bed87720274ddc0
    UNION ALL
    SELECT
      "from" AS delegate,
      "to" AS delegator,
      "evt_block_time" AS dt,
      - CAST(value AS DOUBLE) / POWER(10, 18) AS power
    FROM
      erc20_ethereum.evt_Transfer
      INNER JOIN delegates ON "from" = address
    WHERE
      contract_address = 0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2
      AND "to" <> 0x0a3f6849f78076aefadf113f5bed87720274ddc0
  ),
  fusion AS (
    SELECT
      delegate,
      delegator,
      TRY_CAST(dt AS DATE) AS dt,
      power
    FROM
      delegates_power
    UNION ALL
    SELECT
      stacker AS delegate,
      stacker AS delegator,
      TRY_CAST(dt AS DATE) AS dt,
      power
    FROM
      stackers
    WHERE
      NOT stacker IN (
        SELECT
          address
        FROM
          delegates
      )
  ),
  fill_gaps_1 AS (
    SELECT
      delegate,
      delegator,
      MIN(dt) AS min_dt
    FROM
      fusion
    GROUP BY
      1,
      2
  ),
  fill_gaps_2 AS (
    SELECT
      delegate,
      delegator,
      dt
    FROM
      fill_gaps_1
      CROSS JOIN UNNEST (
        SEQUENCE(
          min_dt,
          CURRENT_DATE,
          interval '1' day
        )
      ) AS _u(dt)
  ),
  fill_gaps AS (
    SELECT
      delegate,
      delegator,
      dt,
      power
    FROM
      fusion
    UNION ALL
    SELECT
      delegate,
      delegator,
      dt,
      0.0 AS power
    FROM
      fill_gaps_2
  ),
  group_by AS (
    SELECT
      delegate,
      delegator,
      dt,
      SUM(power) AS power
    FROM
      fill_gaps
    GROUP BY
      1,
      2,
      3
  ),
  windows AS (
    SELECT
      delegate,
      delegator,
      dt,
      SUM(power) OVER (
        PARTITION BY
          delegate,
          delegator
        ORDER BY
          dt
      ) AS power
    FROM
      group_by
  ),
  delegate_version /* Delegates version */ AS (
    SELECT
      COALESCE(
        d.name,
        COALESCE(p2.name, TRY_CAST(delegate AS VARCHAR))
      ) AS name,
      dt,
      SUM(power) AS power
    FROM
      windows
      LEFT OUTER JOIN delegates AS d ON delegate = d.address
      LEFT OUTER JOIN persons AS p2 ON delegate = p2.address
    GROUP BY
      1,
      2
  ),
  delegate_only /* Delegates only version */ AS (
    SELECT
      COALESCE(d.name, 'Others') AS name,
      dt,
      SUM(power) AS power
    FROM
      windows
      LEFT OUTER JOIN delegates AS d ON delegate = d.address
      LEFT OUTER JOIN persons AS p2 ON delegate = p2.address
    GROUP BY
      1,
      2
  ),
  holder_version /* Holder version */ AS (
    SELECT
      COALESCE(name, TRY_CAST(delegator AS VARCHAR)) AS name,
      dt,
      SUM(power) AS power
    FROM
      windows
      LEFT OUTER JOIN persons AS p ON delegator = p.address
    GROUP BY
      1,
      2
  ),
  version_clean /* Cleanup what below 1k votes */ AS (
    SELECT
      CASE
        WHEN power < 5000 THEN 'Others'
        ELSE name
      END AS delegate,
      dt,
      SUM(power) AS power
    FROM
      holder_version
    GROUP BY
      1,
      2
  )
SELECT
  *
FROM
  version_clean
ORDER BY
  2 DESC NULLS FIRST,
  3 DESC NULLS FIRST