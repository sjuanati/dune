/*
/// @title Investor vesting
/// @db_engine: v2 Dune SQL
/// @purpose Provide aggregated vesting figures for investors
/// @data_validation: can't be done gobally, but per user basis:
///     total = GROInvVesting(0x0537d3DA1Ed1dd7350fF1f3B92b727dfdBAB80f1).totalBalance()
///     vested = GROInvVesting(0x0537d3DA1Ed1dd7350fF1f3B92b727dfdBAB80f1).vestedBalance()
///     vesting = emmm, no function() in contract, but the diff between total - vested ;)
*/

WITH investor_start_date AS (
    SELECT * FROM (VALUES 
        (0xf7d74a3e2295a860cdd88b901940b367737e8a8f, 1632844845),
        (0xf5fb27b912d987b5b6e02a1b1be0c1f0740e2c6f, 1632844845),
        (0xf30026fe8a2c0d01b70b1949ceaf2e09efd8b4a5, 1632844845),
        (0xe73f4e9a51868c5e631c74e6ca5bff357772fbd8, 1632844845),
        (0xde3258c1c45a557f4924d1e4e3d0a4e5341607ee, 1632844845),
        (0xda46892a65820d354d670e3666528595ba03025d, 1632844845),
        (0xce6c0f548be5c79d4bad219863d603c31eb8a847, 1632844845),
        (0xca2a58f421c027e98d41bcb8c4ae019f610dd000, 1632844845),
        (0xc24564be9895a0e4fd60c933280767cc846cba19, 1632844845),
        (0x8175469cbcbe106bef6343e2981537441f26ef2d, 1632844845),
        (0x7fcaf93cc92d51c490fff701fb2c6197497a80db, 1632844845),
        (0x73def31ff47bbd353ac37d5e4ae3ada48a66e0ef, 1632844845),
        (0x66dea2d2639dd894336b06840a26d19bd6a15f5f, 1632844845),
        (0x4b4c13434aac5e99645a4d9e59fe497b3318f50f, 1632844845),
        (0x4a86f8f8b364a6d40c868ebda7255d1231d3958e, 1632844845),
        (0x47ce4acac8fe4e1d6d9bd97099b307f1a6aa8f33, 1632844845),
        (0x3b2d7aa366f35efb450bf7fe7bd2126976c52f97, 1632844845),
        (0x3565fbebc226655adbdcdd49686f29aaf0c78910, 1632844845),
        (0x273131a3ec63ba4fa5f1b10b10883d275c12d1c3, 1632844845),
        (0x19e09fff30e5ec53787278d12de189bae9b60622, 1632844845),
        (0x147488c0401dfd94d84b39f77e8630dd21763d97, 1632844845),
        (0x114b8d7ab033e650003fa3fc72c5ba2d0fd18345, 1632844845)
        ) AS t("investor", "start_date")
    ),
    investor_vests AS (
        SELECT vest."investor" as "investor",
               CAST(vest."amount" AS DOUBLE) / 1e18 as "amount"
        FROM gro_ethereum.GROInvVesting_evt_LogNewVest vest
    ),
    investor_vesting AS (
        SELECT iv."investor" as "investor",
               dates."start_date" as "start_date",
               iv."amount" as "total_amount",
               TO_UNIXTIME(current_timestamp) as "now",
               CASE
                    WHEN TO_UNIXTIME(current_timestamp) < dates."start_date" + 31556952 -- start date + cliff
                        THEN 0
                    WHEN TO_UNIXTIME(current_timestamp) < dates."start_date" + 94670856 -- start date + vesting time
                        THEN iv."amount" * ( TO_UNIXTIME(current_timestamp) - dates."start_date" ) / 94670856
                    ELSE iv."amount"
               END as "vested",
               CASE
                    WHEN TO_UNIXTIME(current_timestamp) < dates."start_date" + 31556952  -- start date + cliff
                        THEN iv."amount"
                    WHEN TO_UNIXTIME(current_timestamp) - 31556952 > dates."start_date"
                        THEN iv."amount" - iv."amount" * ( TO_UNIXTIME(current_timestamp) - dates."start_date" ) / 94670856
                    ELSE 0
               END as "vesting"
    FROM investor_vests iv
        LEFT JOIN investor_start_date dates
        ON iv."investor" = dates."investor"
    )
    
SELECT
    "investor" AS "investor",
    "start_date" AS "startTS",
    from_unixtime("start_date") AS "startDate",
    SUM("total_amount") AS "total",
    SUM("vesting") AS "vesting",
    SUM("vested") AS "vested"
FROM investor_vesting
GROUP BY 1,2
ORDER BY "total" DESC
