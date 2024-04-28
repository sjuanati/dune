-- header
WITH
    overview AS (
        SELECT
            eth.asset_link AS "Asset",
            eth.dt AS "Date",
            eth.txn_count,
            eth.balance * COALESCE(pr.price, 1) AS "Market Cap",
            pr.price AS "Price",
            (COALESCE(pr.price, 1) - 1) * 10000 AS "Peg (bps)",
            eth.balance AS "Total Supply",
            eth.balance_7d_ago,
            IF(ABS(eth.balance_pct_7d) < 1e-4, 0, eth.balance_pct_7d) AS "% Supply 7d",
            eth.balance_30d_ago,
            IF(ABS(eth.balance_pct_30d) < 1e-4, 0, eth.balance_pct_30d) AS "% Supply 30d",
            eth.volume AS "Volume",
            eth.volume_7d_ago,
            IF(ABS(eth.volume_pct_7d) < 1e-4, 0, eth.volume_pct_7d) AS "% Volume 7d",
            eth.volume_30d_ago,
            IF(ABS(eth.volume_pct_30d) < 1e-4, 0, eth.volume_pct_30d) AS "% Volume 30d"
        FROM {{table}} eth
        LEFT JOIN prices.usd_latest pr
            ON eth.address = pr.contract_address AND pr.blockchain = '{{blockchain}}'
    ),
    totals AS (
        SELECT "Date", SUM("Market Cap") AS "Total Market Cap" FROM overview GROUP BY 1
    ),
    totals_30d AS (
        SELECT
            "Date",
            "Total Market Cap",
            COALESCE(LAG("Total Market Cap", 30) OVER (ORDER BY "Date"), 0) AS "Total Market Cap 30D"
        FROM totals
    ),
    totals_last AS (
        SELECT
            "Total Market Cap" / 1000000000 AS "Total Market Cap",
            "Total Market Cap 30D" / 1000000000 AS "Total Market Cap 30D",
            CASE 
                WHEN "Total Market Cap 30D" = 0 THEN 0
                ELSE (("Total Market Cap" - "Total Market Cap 30D") / "Total Market Cap 30D") * 100
            END AS "% Total Market Cap 30D"
        FROM totals_30d
        WHERE "Date" = CURRENT_DATE
    )

SELECT *
FROM overview ov, totals_last l
WHERE ov."Date" = CURRENT_DATE
ORDER BY ov."Market Cap" DESC

