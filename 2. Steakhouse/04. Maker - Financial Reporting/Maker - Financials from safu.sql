WITH
  items AS (
    SELECT
      'PnL' AS item,
      0 AS item_rank,
      '1 - PnL' AS label
    UNION ALL
    SELECT
      'Lending Revenues' AS item,
      1 AS item_rank,
      '1.1 - Lending Revenues' AS label
    UNION ALL
    SELECT
      'Liquidations Revenues' AS item,
      2 AS item_rank,
      '1.2 - Liquidations Revenues' AS label
    UNION ALL
    SELECT
      'Trading Revenues' AS item,
      3 AS item_rank,
      '1.3 - Trading Revenues' AS label
    UNION ALL
    SELECT
      'Lending Expenses' AS item,
      4 AS item_rank,
      '1.4 - Lending Expenses' AS label
    UNION ALL
    SELECT
      'Liquidations Expenses' AS item,
      5 AS item_rank,
      '1.5 - Liquidations Expenses' AS label
    UNION ALL
    SELECT
      'Workforce Expenses' AS item,
      6 AS item_rank,
      '1.6 - Workforce Expenses' AS label
    UNION ALL
    SELECT
      'Net Income' AS item,
      6 AS item_rank,
      '1.9 - Net Income' AS label
    UNION ALL
    SELECT
      'Assets' AS item,
      100 AS item_rank,
      '2 - Assets' AS label
    UNION ALL
    SELECT
      'Crypto Loans' AS item,
      101 AS item_rank,
      '2.1 - Crypto Loans' AS label
    UNION ALL
    SELECT
      'Trading Assets' AS item,
      102 AS item_rank,
      '2.2 - Trading Assets' AS label
    UNION ALL
    SELECT
      'Operating Reserves' AS item,
      108 AS item_rank,
      '2.8 - Operating Reserves' AS label
    UNION ALL
    SELECT
      'Total Assets' AS item,
      199 AS item_rank,
      '2.9 - Total Assets' AS label
    UNION ALL
    SELECT
      'Liabilities & Equity' AS item,
      200 AS item_rank,
      '3 - Liabilities & Equity' AS label
    UNION ALL
    SELECT
      'Liabilities (DAI)' AS item,
      201 AS item_rank,
      '3.1 - Liabilities (DAI)' AS label
    UNION ALL
    SELECT
      'Equity (Surplus Buffer)' AS item,
      207 AS item_rank,
      '3.7 - Equity (Surplus Buffer)' AS label
    UNION ALL
    SELECT
      'Equity (Operating Reserves)' AS item,
      208 AS item_rank,
      '3.8 - Equity (Operating Reserves)' AS label
    UNION ALL
    SELECT
      'Total Liabilities & Equity' AS item,
      299 AS item_rank,
      '3.9 - Total Liabilities & Equity' AS label
  ),
  dao_wallet AS (
    SELECT
      *
    FROM
      (
        VALUES
          (
            0x9e1585d9CA64243CE43D42f7dD7333190F66Ca09,
            'RWF Core Unit Multisig + Operational 1',
            'Fixed',
            'RWF-001'
          ),
          (
            0xD1505ee500791490DE8642353BA6A5b92e3550F7,
            'RWF Core Unit Multisig + Operational 2',
            'Fixed',
            'RWF-001'
          ),
          (
            0xe2c16c308b843eD02B09156388Cb240cEd58C01c,
            'PE Core Unit Multisig + PE Continuous Ops Multisig 1',
            'Fixed',
            'PE-001'
          ),
          (
            0x83e36aaa1c7b99e2d3d07789f7b70fce46f0d45e,
            'PE Core Unit Multisig + PE Continuous Ops Multisig 2',
            'Fixed',
            'PE-001'
          ),
          (
            0x01D26f8c5cC009868A4BF66E268c17B057fF7A73,
            'GovAlpha Multisig',
            'Fixed',
            'GOV-001'
          ),
          (
            0xDCAF2C84e1154c8DdD3203880e5db965bfF09B60,
            'Content Prod Multisig 1',
            'Fixed',
            'OLD-001'
          ),
          (
            0x6a0ce7dbb43fe537e3fd0be12dc1882393895237,
            'Content Prod Multisig 2',
            'Fixed',
            'OLD-001'
          ),
          (
            0x1eE3ECa7aEF17D1e74eD7C447CcBA61aC76aDbA9,
            'GovCom Multisig + Continuous Operation 1',
            'Fixed',
            'COM-001'
          ),
          (
            0x99E1696A680c0D9f426Be20400E468089E7FDB0f,
            'GovCom Multisig + Continuous Operation 2',
            'Fixed',
            'COM-001'
          ),
          (
            0x7800C137A645c07132886539217ce192b9F0528e,
            'Growth Emergency Multisig',
            'Fixed',
            'GRO-001'
          ),
          (
            0xb5eB779cE300024EDB3dF9b6C007E312584f6F4f,
            'SES Multisigs (Permanent Team, Incubation, Grants) 1',
            'Fixed',
            'SES-001'
          ),
          (
            0x7c09Ff9b59BAAebfd721cbDA3676826aA6d7BaE8,
            'SES Multisigs (Permanent Team, Incubation, Grants) 2',
            'Fixed',
            'SES-001'
          ),
          (
            0xf95eB8eC63D6059bA62b0A8A7F843c7D92f41de2,
            'SES Multisigs (Permanent Team, Incubation, Grants) 3',
            'Fixed',
            'SES-001'
          ),
          (
            0xd98ef20520048a35EdA9A202137847A62120d2d9,
            'Risk Multisig',
            'Fixed',
            'RISK-001'
          ),
          (
            0x8Cd0ad5C55498Aacb72b6689E1da5A284C69c0C7,
            'DUX Team Wallet',
            'Fixed',
            'DUX-001'
          ),
          (
            0x6D348f18c88D45243705D4fdEeB6538c6a9191F1,
            'StarkNet Team Wallet',
            'Fixed',
            'SNE-001'
          ),
          (
            0x955993Df48b0458A01cfB5fd7DF5F5DCa6443550,
            'Strategic Happiness Wallet 1',
            'Fixed',
            'SH-001'
          ),
          /* prior primary wallet, still uses for smaller payments */ (
            0xc657ac882fb2d6ccf521801da39e910f8519508d,
            'Strategic Happiness Wallet 2',
            'Fixed',
            'SH-001'
          ),
          /* multisig for most expenses */ (
            0xD740882B8616B50d0B317fDFf17Ec3f4f853F44f,
            'CES Team Wallet',
            'Fixed',
            'CES-001'
          ),
          (
            0x56349A38e09f36039f6AF77309690d217Beaf0bF,
            'DECO Ops + DECO Protocol Wallets 1',
            'Fixed',
            'DECO-001'
          ),
          (
            0xA78F1F5698f8d345a14d7323745C6c56fB8227F0,
            'DECO Ops + DECO Protocol Wallets 2',
            'Fixed',
            'DECO-001'
          ),
          (
            0x465AA62a82E220B331f5ECcA697c20E89554B298,
            'SAS Team Wallet',
            'Fixed',
            'SAS-001'
          ),
          (
            0x124c759D1084E67B19a206ab85c4527Fab26c342,
            'IS Ops Wallet',
            'Fixed',
            'IS-001'
          ),
          (
            0x7327Aed0Ddf75391098e8753512D8aEc8D740a1F,
            'Data Insights Wallet',
            'Fixed',
            'DIN-001'
          ),
          (
            0x2dC0420A736D1F40893B9481D8968E4D7424bC0B,
            'TechOps',
            'Fixed',
            'TECH-001'
          ),
          (
            0x2B6180b413511ce6e3DA967Ec503b2Cc19B78Db6,
            'Oracle Gas Cost Multisig + Emergency Fund 1',
            'Variable',
            'GAS'
          ),
          (
            0x1A5B692029b157df517b7d21a32c8490b8692b0f,
            'Oracle Gas Cost Multisig + Emergency Fund 2',
            'Variable',
            'GAS'
          ),
          (
            0x53CCAA8E3beF14254041500aCC3f1D4edb5B6D24,
            'Oracle Multisig, Emergency Multisig 1',
            'Fixed',
            'ORA-001'
          ),
          (
            0x2d09B7b95f3F312ba6dDfB77bA6971786c5b50Cf,
            'Oracle Multisig, Emergency Multisig 2',
            'Fixed',
            'ORA-001'
          ),
          (
            0xf737C76D2B358619f7ef696cf3F94548fEcec379,
            'Strategic Finance Multisig',
            'Fixed',
            'SF-001'
          ),
          (
            0x3d274fbac29c92d2f624483495c0113b44dbe7d2,
            'Events Multisig',
            'Fixed',
            'EVENTS-001'
          ),
          (
            0x34d8d61050ef9d2b48ab00e6dc8a8ca6581c5d63,
            'Foundation Operational Wallet',
            'Fixed',
            'DAIF-001'
          ),
          (
            0xbe8e3e3618f7474f8cb1d074a26affef007e98fb,
            'DS Pause Proxy',
            'Variable',
            'DSPP'
          ),
          (
            0x73f09254a81e1f835ee442d1b3262c1f1d7a13ff,
            'Interim Multisig',
            'Fixed',
            'INTERIM'
          ),
          (
            0x87AcDD9208f73bFc9207e1f6F0fDE906bcA95cc6,
            'SES Multisig (Auditor)',
            'Fixed',
            'SES-001'
          ),
          (
            0x5A994D8428CCEbCC153863CCdA9D2Be6352f89ad,
            'DUX Auditor Wallet',
            'Fixed',
            'DUX-001'
          ),
          (
            0x25307aB59Cd5d8b4E2C01218262Ddf6a89Ff86da,
            'CES Auditor Wallet',
            'Fixed',
            'CES-001'
          ),
          (
            0xf482d1031e5b172d42b2daa1b6e5cbf6519596f7,
            'DECO Auditor Wallet',
            'Fixed',
            'DECO-001'
          ),
          (
            0xb1f950a51516a697e103aaa69e152d839182f6fe,
            'SAS Auditor Wallet',
            'Fixed',
            'SAS-001'
          ),
          (
            0xd1f2eef8576736c1eba36920b957cd2af07280f4,
            'IS Auditor Wallet',
            'Fixed',
            'IS-001'
          ),
          (
            0x96d7b01Cc25B141520C717fa369844d34FF116ec,
            'RWF Auditor Wallet',
            'Fixed',
            'RWF-001'
          ),
          (
            0x1a3da79ee7db30466ca752de6a75def5e635b2f6,
            'TechOps Auditor Wallet',
            'Fixed',
            'TECH-001'
          ),
          (
            0x5F5c328732c9E52DfCb81067b8bA56459b33921f,
            'Foundation Reserves',
            'Fixed',
            'DAIF-001'
          ),
          (
            0x478c7ce3e1df09130f8d65a23ad80e05b352af62,
            'Gelato Keepers',
            'Variable',
            'GELATO'
          )
      ) AS t (wallet_address, wallet_label, varfix, code)
), chart_of_accounts
    (code, primary_label, secondary_label, account_label, category_label, subcategory_label)
    AS (values
    (11110, 'Assets', 'Collateralized Lending', 'Crypto-Loans', 'ETH', 'ETH'),
    (11120, 'Assets', 'Collateralized Lending', 'Crypto-Loans', 'BTC', 'BTC'),
    (11130, 'Assets', 'Collateralized Lending', 'Crypto-Loans', 'WSTETH', 'WSTETH'),
    (11140, 'Assets', 'Collateralized Lending', 'Crypto-Loans', 'Liquidity Pool', 'Stable LP'),
    (11141, 'Assets', 'Collateralized Lending', 'Crypto-Loans', 'Liquidity Pool', 'Volatile LP'),
    (11199, 'Assets', 'Collateralized Lending', 'Crypto-Loans', 'Other', 'Other'),
    (11210, 'Assets', 'Collateralized Lending', 'Money Market', 'Money Market', 'D3M'),
    (11510, 'Assets', 'Collateralized Lending', 'Legacy', 'Stablecoins', 'Stablecoins'),
    (12310, 'Assets', 'Real-World Lending', 'RWA', 'Private Credit RWA', 'Off-Chain Private Credit'),
    (12311, 'Assets', 'Real-World Lending', 'RWA', 'Private Credit RWA', 'Tokenized Private Credit'),
    (12320, 'Assets', 'Real-World Lending', 'RWA', 'Public Credit RWA', 'Off-Chain Public Credit'), 
    (12321, 'Assets', 'Real-World Lending', 'RWA', 'Public Credit RWA', 'Tokenized Public Credit'),
    (13410, 'Assets', 'Liquidity Pool', 'PSM', 'PSM', 'Non-Yielding Stablecoin'),
    (13411, 'Assets', 'Liquidity Pool', 'PSM', 'PSM', 'Yielding Stablecoin'),
    (14620, 'Assets', 'Proprietary Treasury', 'Holdings', 'Treasury Assets', 'DS Pause Proxy'),
    (19999, 'Assets', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token'),
    
    (21110, 'Liabilities', 'Stablecoin', 'Circulating', 'Interest-bearing', 'Dai'),
    (21120, 'Liabilities', 'Stablecoin', 'Circulating', 'Non-interest bearing', 'Dai'),
    (29999, 'Liabilities', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token'),

    (31110, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'ETH', 'ETH SF'),
    (31120, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'BTC', 'BTC SF'),
    (31130, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'WSTETH', 'WSTETH SF'),
    (31140, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'Liquidity Pool', 'Stable LP SF'),
    (31141, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'Liquidity Pool', 'Volatile LP SF'),
    (31150, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'Other', 'Other SF'),
    (31160, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'Money Market', 'D3M SF'),
    (31170, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'RWA', 'Off-Chain Private Credit SF'),
    (31171, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'RWA', 'Tokenized Private Credit SF'),
    (31172, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'RWA', 'Off-Chain Public Credit Interest'),
    (31173, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'RWA', 'Tokenized Public Credit Interest'),
    (31180, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'PSM', 'Yielding Stablecoin Interest'),
    (31190, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'Stablecoins', 'Stablecoins SF'),
    (31210, 'Equity', 'Protocol Surplus', 'Liquidation Revenues', 'Liquidation Revenues', 'Liquidation Revenues'),
    (31310, 'Equity', 'Protocol Surplus', 'Trading Revenues', 'Trading Revenues', 'Trading Revenues'),
    --(31311, 'Equity', 'Protocol Surplus', 'Trading Revenues', 'Trading Revenues', 'Teleport Revenues'),  --needs to be added still
    (31410, 'Equity', 'Protocol Surplus', 'MKR Mints Burns', 'MKR Mints', 'MKR Mints'),
    (31420, 'Equity', 'Protocol Surplus', 'MKR Mints Burns', 'MKR Burns', 'MKR Burns'),
    (31510, 'Equity', 'Protocol Surplus', 'Sin', 'Sin Inflow', 'Sin Inflow'),
    (31520, 'Equity', 'Protocol Surplus', 'Sin', 'Sin Outflow', 'Sin Outflow'),
    (31610, 'Equity', 'Protocol Surplus', 'Direct Expenses', 'DSR', 'Circulating Dai'),
    (31620, 'Equity', 'Protocol Surplus', 'Direct Expenses', 'Liquidation Expenses', 'Liquidation Expenses'),
    (31630, 'Equity', 'Protocol Surplus', 'Direct Expenses', 'Oracle Gas Expenses', 'Oracle Gas Expenses'),
    (31710, 'Equity', 'Protocol Surplus', 'Indirect Expenses', 'Keeper Maintenance', 'Keeper Maintenance'),
    (31720, 'Equity', 'Protocol Surplus', 'Indirect Expenses', 'Workforce Expenses', 'Workforce Expenses'),
    (31730, 'Equity', 'Protocol Surplus', 'Indirect Expenses', 'Workforce Expenses', 'Returned Workforce Expenses'),
    (31740, 'Equity', 'Protocol Surplus', 'Indirect Expenses', 'Direct to Third Party Expenses', 'Direct to Third Party Expenses'),
    (31810, 'Equity', 'Reserved MKR Surplus', 'MKR Token Expenses', 'Direct MKR Token Expenses', 'Direct MKR Token Expenses'),
    (32810, 'Equity', 'Proprietary Treasury', 'Holdings', 'Treasury Assets', 'DS Pause Proxy'),
    (33110, 'Equity', 'Reserved MKR Surplus', 'MKR Token Expenses', 'Vested MKR Token Expenses', 'Vested MKR Token Expenses'),
    (34110, 'Equity', 'Reserved MKR Surplus', 'MKR Contra Equity', 'MKR Contra Equity', 'MKR Contra Equity'), 
    (39999, 'Equity', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token')
),
  periods
  AS (
    SELECT
      TRY_CAST(period AS DATE) as period,
      YEAR(
        CAST(SUBSTR(CAST(period AS VARCHAR), 1, 10) AS DATE)
      ) AS year,
      MONTH(
        CAST(SUBSTR(CAST(period AS VARCHAR), 1, 10) AS DATE)
      ) AS month
    FROM
      (
        SELECT
          period
        FROM
          UNNEST (
            SEQUENCE(
              CAST(
                SUBSTR(
                  CAST(CAST('2019-11-01' AS TIMESTAMP) AS VARCHAR),
                  1,
                  10
                ) AS DATE
              ),
              CURRENT_DATE,
              INTERVAL '1' MONTH
            )
          ) AS array_column (period)
      )
  ),
accounting AS
(
    SELECT year
    , month
    , token
    , code
    , primary_label
    , secondary_label
    , account_label
    , category_label
    , subcategory_label
    , SUM(value) AS sum_value
    FROM 
    (
        SELECT YEAR(ts) AS year
        , month(ts) AS month
        , token
        , code
        , primary_label
        , secondary_label
        , account_label
        , category_label
        , subcategory_label
        , value
        FROM dune.steakhouse.result_maker_accounting
        LEFT JOIN chart_of_accounts
        USING (code)
        
        UNION ALL
        
        SELECT year
        , month
        , token
        , chart_of_accounts.code
        , primary_label
        , secondary_label
        , account_label
        , category_label
        , subcategory_label
        , 0 AS value
        FROM periods
        CROSS JOIN chart_of_accounts
        LEFT JOIN (SELECT token, code FROM dune.steakhouse.result_maker_accounting GROUP BY 1,2) tokens
        ON chart_of_accounts.code = tokens.code
    )
    GROUP BY 1,2,3,4,5,6,7,8,9
), accounting2 AS
(
    SELECT a.year
    , a.month
    , a.token
    , a.code
    , a.primary_label
    , a.secondary_label
    , a.account_label
    , a.category_label
    , a.subcategory_label
    , CASE WHEN a.code = 31210 THEN --netting liquidation revenues ane expenses and applying them to appropriate category
            CASE WHEN COALESCE(a.sum_value,0) + COALESCE(b.sum_value,0) > 0 THEN COALESCE(a.sum_value,0) + COALESCE(b.sum_value,0) ELSE 0 END
        WHEN a.code = 31620 THEN
            CASE WHEN COALESCE(a.sum_value,0) + COALESCE(b.sum_value,0) > 0 THEN 0 ELSE COALESCE(a.sum_value,0) + COALESCE(b.sum_value,0) END
        ELSE a.sum_value END AS sum_value
    , SUM(CASE WHEN a.code = 31210 THEN --netting liquidation revenues ane expenses and applying them to appropriate category
            CASE WHEN COALESCE(a.sum_value,0) + COALESCE(b.sum_value,0) > 0 THEN COALESCE(a.sum_value,0) + COALESCE(b.sum_value,0) ELSE 0 END
        WHEN a.code = 31620 THEN
            CASE WHEN COALESCE(a.sum_value,0) + COALESCE(b.sum_value,0) > 0 THEN 0 ELSE COALESCE(a.sum_value,0) + COALESCE(b.sum_value,0) END
        ELSE a.sum_value END) OVER (PARTITION BY a.code ORDER BY a.year, a.month) AS cum_value
    FROM accounting a
    LEFT JOIN accounting b
    ON a.year = b.year
    AND a.month = b.month
    AND ( (a.code = 31210 AND b.code = 31620) OR (a.code = 31620 AND b.code = 31210) )
)
SELECT
  YEAR(
    CAST(SUBSTR(CAST(period AS VARCHAR), 1, 10) AS DATE)
  ) AS year,
  MONTH(
    CAST(SUBSTR(CAST(period AS VARCHAR), 1, 10) AS DATE)
  ) AS month,
  SUBSTRING(cast(period as varchar), 1, 7) AS period,
  label AS item,
  SUM(
    CASE item
      WHEN 'Liquidations Revenues' THEN CASE WHEN category_label = 'Liquidation Revenues' THEN sum_value END
      WHEN 'Liquidations Expenses' THEN CASE WHEN category_label = 'Liquidation Expenses' THEN sum_value END
      WHEN 'Trading Revenues' THEN CASE WHEN category_label = 'Trading Revenues' THEN sum_value WHEN subcategory_label = 'Stablecoins SF' THEN sum_value END
      WHEN 'Lending Revenues' THEN CASE WHEN account_label = 'Gross Interest Revenues' AND category_label <> 'Stablecoins' THEN sum_value END /* should psm yield go here or into trading revenues? */
      WHEN 'Lending Expenses' THEN CASE WHEN category_label = 'DSR' THEN sum_value END
      WHEN 'Workforce Expenses' THEN CASE WHEN account_label = 'Indirect Expenses' THEN sum_value WHEN category_label = 'Oracle Gas Expenses' THEN sum_value END
      WHEN 'Net Income' THEN CASE WHEN account_label IN ('Gross Interest Revenues', 'Liquidation Revenues', 'Trading Revenues', 'Direct Expenses', 'Indirect Expenses') THEN sum_value END
      WHEN 'Crypto Loans' THEN CASE WHEN secondary_label IN ('Collateralized Lending', 'Real-World Lending') THEN cum_value END /* + COALESCE(d3m_revenues, 0) --d3m revenues flow back in a different way so I don't think this needs to be here any longer. if it does it would need to pull estimated earnings and only those that have occured since the last time d3m revenue was realized. will need to explore more */
      WHEN 'Trading Assets' THEN CASE WHEN secondary_label = 'Liquidity Pool' THEN cum_value END
      --WHEN 'Operating Reserves' THEN assets_operating_wallets --this no longer exists
      WHEN 'Total Assets' THEN CASE WHEN secondary_label IN ('Collateralized Lending', 'Real-World Lending', 'Liquidity Pool') THEN cum_value END -- + COALESCE(assets_operating_wallets, 0) --no longer
      WHEN 'Liabilities (DAI)' THEN CASE WHEN subcategory_label = 'Dai' THEN cum_value END
      WHEN 'Equity (Surplus Buffer)' THEN CASE WHEN secondary_label = 'Protocol Surplus' THEN cum_value END
      --WHEN 'Equity (Operating Reserves)' THEN assets_operating_wallets --this no longer exists
      WHEN 'Total Liabilities & Equity' THEN CASE WHEN subcategory_label = 'Dai' THEN cum_value WHEN secondary_label = 'Protocol Surplus' THEN cum_value END --+ COALESCE(assets_operating_wallets, 0)
    END
  ) AS value,
  SUM(
    CASE item
      WHEN 'Net Income' THEN CASE WHEN category_label IN ('Liquidation Revenues', 'Liquidation Expenses') THEN sum_value END
      WHEN 'PnL' THEN CASE WHEN category_label IN ('Liquidation Revenues', 'Liquidation Expenses') THEN sum_value END
    END
  ) AS liquidation_income,
  SUM(
    CASE item
      WHEN 'Net Income' THEN CASE WHEN category_label = 'Trading Revenues' THEN sum_value WHEN subcategory_label = 'Stablecoins SF' THEN sum_value END
      WHEN 'PnL' THEN CASE WHEN category_label = 'Trading Revenues' THEN sum_value WHEN subcategory_label = 'Stablecoins SF' THEN sum_value END
    END
  ) AS trading_income,
  SUM(
    CASE item
      WHEN 'Net Income' THEN CASE WHEN account_label = 'Gross Interest Revenues' AND category_label <> 'Stablecoins' THEN sum_value WHEN category_label = 'DSR' THEN sum_value END
      WHEN 'PnL' THEN CASE WHEN account_label = 'Gross Interest Revenues' AND category_label <> 'Stablecoins' THEN sum_value WHEN category_label = 'DSR' THEN sum_value END
    END
  ) AS lending_income,
  /*SUM(
    CASE item
      WHEN 'Net Income' THEN COALESCE(liquidation_revenues, 0) + COALESCE(- liquidation_expenses, 0) + COALESCE(trading_revenues, 0) + COALESCE(stablecoin_lending_revenues, 0) + COALESCE(lending_revenues, 0) + COALESCE(- lending_expenses, 0) + COALESCE(d3m_revenues, 0) + COALESCE(psm_yield, 0) + COALESCE(hvb_yield, 0)
      WHEN 'PnL' THEN COALESCE(liquidation_revenues, 0) + COALESCE(- liquidation_expenses, 0) + COALESCE(trading_revenues, 0) + COALESCE(stablecoin_lending_revenues, 0) + COALESCE(lending_revenues, 0) + COALESCE(- lending_expenses, 0) + COALESCE(d3m_revenues, 0) + COALESCE(psm_yield, 0) + COALESCE(hvb_yield, 0)
    END
  ) AS total_revenue,*/
  SUM(
    CASE item
      WHEN 'Workforce Expenses' THEN CASE WHEN account_label = 'Indirect Expenses' THEN sum_value WHEN category_label = 'Oracle Gas Expenses' THEN sum_value END
      WHEN 'PnL' THEN CASE WHEN account_label = 'Indirect Expenses' THEN sum_value WHEN category_label = 'Oracle Gas Expenses' THEN sum_value END
    END
  ) AS expenses,
  SUM(
    CASE item
      WHEN 'Net Income' THEN CASE WHEN account_label IN ('Gross Interest Revenues', 'Liquidation Revenues', 'Trading Revenues', 'Direct Expenses', 'Indirect Expenses') THEN sum_value END
      WHEN 'PnL' THEN CASE WHEN account_label IN ('Gross Interest Revenues', 'Liquidation Revenues', 'Trading Revenues', 'Direct Expenses', 'Indirect Expenses') THEN sum_value END
    END
  ) AS net_income
FROM
  periods
  CROSS JOIN items
  LEFT JOIN accounting2 USING (year, month)
GROUP BY
  1,
  2,
  3,
  4
ORDER BY
  3 DESC,
  4 NULLS FIRST