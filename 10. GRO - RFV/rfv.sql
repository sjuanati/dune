WITH
    vests AS (
        SELECT
            "user" AS "user",
            CAST("amount" AS DOUBLE) / 1e18 as "amount",
            FROM_UNIXTIME(CAST(json_extract_scalar(json_parse("vesting"), '$.startTime') AS INTEGER)) as "startTime"
        FROM gro_ethereum.GROVestingV2_evt_LogVest 
        WHERE FROM_UNIXTIME(CAST(json_extract_scalar(json_parse("vesting"), '$.startTime') AS INTEGER)) >= date('2023-09-05')
    ),
    vests_total AS (
        SELECT sum(amount) AS "total" FROM vests
    )
    
SELECT * FROM "vests", "vests_total" order by 2 DESC
