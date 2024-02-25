/*
-- @title: ENS - Registrations per day
-- @description: Counts daily ENS domain registrations and renewals over the last 90 days.
-- @author: Steakhouse Financial
-- @notes: N/A
-- @version:
    - 2.0 - 2024-02-18 - Added comment header, added ETH Registrar 4, removed cash data (already shown
    in query 'ENS - Revenues per day')
    - 1.0 - ????-??-?? - Initial version
*/

WITH
  events AS (
    -- New Registrations
    SELECT evt_block_time, 'new' AS type
    FROM ethereumnameservice_ethereum."ETHRegistrarController_1_evt_NameRegistered"
    UNION ALL
    SELECT evt_block_time, 'new' AS type
    FROM ethereumnameservice_ethereum."ETHRegistrarController_2_evt_NameRegistered"
    UNION ALL
    SELECT evt_block_time, 'new' AS type
    FROM ethereumnameservice_ethereum."ETHRegistrarController_3_evt_NameRegistered"
    UNION ALL
    SELECT evt_block_time, 'new' AS type
    FROM ethereumnameservice_ethereum."ETHRegistrarController_4_evt_NameRegistered"
    UNION ALL
    -- Registration Renewals
    SELECT evt_block_time, 'renew'
    FROM ethereumnameservice_ethereum."ETHRegistrarController_1_evt_NameRenewed"
    UNION ALL
    SELECT evt_block_time, 'renew'
    FROM ethereumnameservice_ethereum."ETHRegistrarController_2_evt_NameRenewed"
    UNION ALL
    SELECT evt_block_time, 'renew'
    FROM ethereumnameservice_ethereum."ETHRegistrarController_3_evt_NameRenewed"
    UNION ALL
    SELECT evt_block_time, 'renew'
    FROM ethereumnameservice_ethereum."ETHRegistrarController_4_evt_NameRenewed"
  ),
  aggregated AS (
    SELECT CAST(evt_block_time AS DATE) AS period, type, COUNT(*) AS cnt FROM events GROUP BY 1, 2
  ),
  summary AS (
    SELECT
      period,
      SUM(CASE WHEN type = 'new' THEN cnt ELSE 0 END) AS new_cnt,
      SUM(CASE WHEN type = 'renew' THEN cnt ELSE 0 END) AS renew_cnt
    FROM aggregated
    GROUP BY 1
  )

SELECT
    period,
    new_cnt,
    renew_cnt,
    new_cnt + COALESCE(renew_cnt, 0) AS cnt
FROM summary
WHERE period > CURRENT_DATE - interval '90' day
ORDER BY 1 DESC;
