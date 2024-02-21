/*
-- @title: Steakhouse - ENS - Table Updates
-- @description: Checks whether all dashboard components are refreshed according to their period granurality (daily, monthly..)
-- @author: Steakhouse Financial
-- @notes: N/A
    - TODO: Some MVs might be scheduled at different times, so 24h to 27h window needs to be confirmed
-- @version:
    - 1.1 - 2024-02-19 - Added hyperlinks to all dashboard queries
    - 1.0 - 2024-02-15 - Initial release
*/

WITH
    latest_updates AS (
        SELECT
            'Profit & Loss <a href="https://dune.com/queries/3182452" target="_blank"> 🔗 </a>' AS "Visualization",
            'Monthly' AS "Refresh",
            MAX(period) AS "Period"
        FROM query_3182452 -- ENS - PnL
        WHERE period > date '2023-12-01'
        UNION ALL
        SELECT
            'Balance Sheet <a href="https://dune.com/queries/2840252" target="_blank"> 🔗 </a>' AS "Visualization",
            'Daily' AS "Refresh",
            MAX(period) AS "Period"
        FROM query_2840252 -- ENS - Balance Sheet
        WHERE period > date '2024-01-01'
        UNION ALL
        -- TODO
        --SELECT MAX(?), 'Yearly Balance Sheet Table' FROM query_2840098
        --UNION ALL
        SELECT
            'Revenues per day <a href="https://dune.com/queries/3069494" target="_blank"> 🔗 </a>' AS "Visualization",
            'Daily' AS "Refresh",
            MAX(period) AS "Period"
        FROM query_3069494 -- ENS - Revenues per day
        WHERE period > date '2024-01-01'
        UNION ALL
        SELECT
            'Holdings over time per wallet <a href="https://dune.com/queries/1778942" target="_blank"> 🔗 </a>' AS "Visualization",
            'Daily' AS "Refresh",
            MAX(period) AS "Period"
        FROM query_1778942 -- ENS - Holdings over time per wallet
        WHERE period > date '2024-01-01'
        UNION ALL
        SELECT
            'Cashflows per day (Financials) <a href="https://dune.com/queries/3069516" target="_blank"> 🔗 </a>' AS "Visualization",
            'Daily' AS "Refresh",
            MAX(period) AS "Period"
        FROM query_3069516 -- ENS - Cashflows per day
        WHERE period > date '2024-01-01'
        UNION ALL
        SELECT
            'Holdings over time <a href="https://dune.com/queries/1757839" target="_blank"> 🔗 </a>' AS "Visualization",
            'Daily' AS "Refresh",
            MAX(period) AS "Period"
        FROM query_1757839 -- ENS - Holdings over time
        WHERE period > date '2024-01-01'
        UNION ALL
        SELECT
            'Cashflows per day (Activity) <a href="https://dune.com/queries/1347864" target="_blank"> 🔗 </a>' AS "Visualization",
            'Daily' AS "Refresh",
            MAX(period) AS "Period"
        FROM query_1347864 -- ENS - Cashflows per day
        WHERE period > date '2024-01-01'
        UNION ALL
        SELECT
            'Endowment <a href="https://dune.com/queries/2840308" target="_blank"> 🔗 </a>' AS "Visualization",
            'Daily' AS "Refresh",
            MAX(period) AS "Period"
        FROM query_2840308 -- ENS - Endowment
        WHERE period > date '2024-01-01'
        UNION ALL
        SELECT
            'Price <a href="https://dune.com/queries/1355395" target="_blank"> 🔗 </a>' AS "Visualization",
            'Daily' AS "Refresh",
            MAX(period) AS "Period"
        FROM query_1355395 -- ENS - Price
        WHERE period > date '2024-01-01'
    ),
    status AS (
        SELECT
            "Visualization",
            "Refresh",
            "Period",
            CASE
                WHEN "Refresh" = 'Daily' THEN
                    CASE
                        WHEN CURRENT_DATE - period > interval '27' hour THEN '🔴'  -- data older than 27 hours
                        WHEN CURRENT_DATE - period > interval '24' hour THEN '🟠'  -- data within 24-27 hours
                        ELSE '🟢'  -- data within the last 24 hours
                    END
                WHEN "Refresh" = 'Monthly' THEN
                    CASE
                        WHEN DATE_TRUNC('month', CURRENT_DATE) - interval '1' month = DATE_TRUNC('month', "Period") THEN '🟢' -- within previous month
                        ELSE '🔴' -- older than a month
                    END
                ELSE 'Unknown'
            END AS "Status"
        FROM latest_updates
    )

SELECT
    "Visualization",
    "Refresh",
    "Period",
    "Status"
FROM status
ORDER BY "Visualization";
