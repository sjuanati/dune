-- header
WITH
    overview AS (
        SELECT
            t.asset as asset_name,
            t.asset_link AS "Asset",
            t.dt AS "Date",
            t.category,
            t.txn_count,
            t.balance * COALESCE(pr.price, 1) AS "Market Cap",
            pr.price AS "Price",
            (pr.price - 1) * 10000 AS "Peg (bps)",
            t.balance AS "Total Supply",
            t.balance_7d_ago,
            IF(ABS(t.balance_pct_7d) < 1e-4, 0, t.balance_pct_7d) AS "% Supply 7d",
            t.balance_30d_ago,
            IF(ABS(t.balance_pct_30d) < 1e-4, 0, t.balance_pct_30d) AS "% Supply 30d",
            t.volume AS "Volume",
            t.volume_7d_ago,
            IF(ABS(t.volume_pct_7d) < 1e-4, 0, t.volume_pct_7d) AS "% Volume 7d",
            t.volume_30d_ago,
            IF(ABS(t.volume_pct_30d) < 1e-4, 0, t.volume_pct_30d) AS "% Volume 30d"
        FROM {{table}} t
        LEFT JOIN prices.usd_latest pr
            ON t.address = pr.contract_address AND pr.blockchain = '{{blockchain}}'
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
