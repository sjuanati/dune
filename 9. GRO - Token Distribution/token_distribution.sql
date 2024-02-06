/*
/// @title    GRO Token Distribution
/// @purpose  Understand the total GRO that could potentially be used towards governance
/// @engine   v2 Dune SQL
/// @Version  3.0
/// @kpi    - quota: GRO token allocation according to GRO Tokenomics
///         - total: liquid + vesting + vested GRO
///         - liquid: minted GRO in circulation
///         - vesting/ed: vesting + vested GRO
///         - vesting: locked GRO in GROVesting, GROTeamVesting & GROInvVesting contracts
///         - vested: unlocked GRO from above contracts excluding claims
///         - gro price: current dollar value of GRO based on Uniswap GRO/USDC pool
/// @dev    - Vested GRO for community deducts claims via LogExit, whereas vested GRO for team &
///           investors deducts claims via LogClaimed.
///         - Treasury is calculated as the minted GRO to wallet 0x..fe0d
///         - startDate for team & investor contracts is not emmited through any event, so it must be
///           manually added in the Dashboard queries when a new vesting position is created. To check
///           for new positions, refer to Checksum table in https://dune.com/wint3rmute/gro-vesting-g2
/// @checks   data validation for vesting/ed can't be done gobally, but per user basis:
///           - vesting/ed = GROTeamVesting.positionBalance()
///           - vested = GROTeamVesting.positionVestedBalance()
///           - vesting = no function() in contract, but the diff between vesting/ed - vested
///           (same for investors)
/// @contracts
///         - GROVesting: 0xA28693bf01Dc261887b238646Bb9636cB3a3730B
///         - GROVestingV2: 0x748218256AfE0A19a88EBEB2E0C5Ce86d2178360
///         - GROTeamVesting: 0xF43c6bDD2F9158B5A78DCcf732D190C490e28644
///         - GROInvVesting: 0x0537d3DA1Ed1dd7350fF1f3B92b727dfdBAB80f1
///         - GROToken: 0x3Ec8798B81485A254928B70CDA1cf0A2BB0B74D7
///         - UniswapV2.Pair: 0x21C5918CcB42d20A2368bdCA8feDA0399EbfD2f6
///         - Treasury wallet: 0x359f4fe841f246a095a82cb26f5819e10a91fe0d
*/

WITH
    /***********************************************************************************************
    ****************************** C O M M U N I T Y  -  GROVesting ********************************
    ***********************************************************************************************/
    vests AS (
        SELECT
            CAST("amount" AS DOUBLE) / 1e18 AS "amount"
            --CAST(json_extract_scalar(json_parse("vesting"), '$.startTime') AS INTEGER) as startTime
        FROM gro_ethereum.GROVesting_evt_LogVest
        UNION ALL
        SELECT
            CAST("amount" AS DOUBLE) / 1e18 AS "amount"
            --CAST(json_extract_scalar(json_parse("vesting"), '$.startTime') AS INTEGER) as startTime
        FROM gro_ethereum.GROVestingV2_evt_LogVest
        UNION ALL
        SELECT
            -CAST("unlocked" AS DOUBLE) / 1e18 - CAST("penalty" AS DOUBLE) / 1e18 AS "amount"
            --0 as startTime
        FROM gro_ethereum.GROVesting_evt_LogExit
        UNION ALL
        SELECT
            -CAST("unlocked" AS DOUBLE) / 1e18 - CAST("penalty" AS DOUBLE) / 1e18 AS "amount"
            --0 as startTime
        FROM gro_ethereum.GROVestingV2_evt_LogExit
        UNION ALL
        SELECT
            -CAST("mintingAmount" AS DOUBLE) / 1e18 - CAST("penalty" AS DOUBLE) / 1e18 AS "amount"
        FROM gro_ethereum.GROVestingV2_evt_LogInstantExit
    ),
    rewards_vesting_totals AS (
        SELECT
            SUM("amount") AS "total",
            0 AS "vesting",
            SUM("amount") AS "vested"
        FROM vests
    ),
    /***********************************************************************************************
    ********************************* T E A M  -  GROTeamVesting ***********************************
    ***********************************************************************************************/
    team_start_date AS (
      SELECT * FROM query_3407758
    ),
    team_vests AS (
        SELECT 
            CAST(vest."id" AS INTEGER) as "id",
            vest."contributor" as "contributor",
            CASE
                WHEN stop_vest."contributor" IS NULL
                    THEN CAST(vest."amount" AS DOUBLE) / 1e18
                    ELSE CAST(stop_vest."unlocked" AS DOUBLE) / 1e18
                END
            AS "amount"
        FROM gro_ethereum.GROTeamVesting_evt_LogNewVest vest
        LEFT JOIN gro_ethereum.GROTeamVesting_evt_LogStoppedVesting stop_vest
            ON vest."contributor" = stop_vest."contributor"
            AND vest."id" = stop_vest."id"
    ),
    -- All team vesting positions were stopped on 4 October 2023, so 3-year vesting date is
    -- replaced by timestamp 1696454100 (04/10/2023)
    team_vesting as (
         SELECT tv."contributor" as "contributor",
                tv."id" as "id",
                current_timestamp as "current_ts",
                dates."start_date" as "start_date",
                tv."amount" as "total_gro",
                TO_UNIXTIME(current_timestamp) as "now",
                CASE
                    WHEN TO_UNIXTIME(current_timestamp) < dates."start_date" + 31556952 -- start date + cliff
                        THEN 0
                    WHEN TO_UNIXTIME(current_timestamp) < dates."start_date" + 94670856 -- start date + vesting time
                        --THEN tv."amount" * ( TO_UNIXTIME(current_timestamp) - dates."start_date" ) / 94670856
                        THEN tv."amount" * ( 1696454100 - dates."start_date" ) / 94670856
                    ELSE tv."amount"
                END as "vested_gro",
                --CASE
                --    WHEN TO_UNIXTIME(current_timestamp) < dates."start_date" + 31556952  -- start date + cliff
                --        THEN tv.amount
                --    WHEN TO_UNIXTIME(current_timestamp) - 31556952 > dates."start_date"
                --        THEN tv."amount" - tv."amount" * ( TO_UNIXTIME(current_timestamp) - dates."start_date" ) / 94670856
                --    ELSE 0
                --END as "vesting_gro"
                0 AS "vesting_gro"
        FROM team_vests tv
            LEFT JOIN team_start_date dates
            ON tv."contributor" = dates."contributor"
            AND tv."id" = dates."id"
    ),
    team_vesting_totals AS (
        SELECT
            SUM("total_gro") AS "total",
            SUM("vesting_gro") AS "vesting",
            SUM("vested_gro") AS "vested"
        FROM team_vesting
    ),
    -- total GRO unlocked based on => (QUOTA) * (block.timestamp - VESTING_START_TIME) / (VESTING_TIME)
    team_unlocked AS (
        SELECT (22509423 * ( (FLOOR(TO_UNIXTIME("time"))) - 1632844800) / 94670856) AS "amount"
        FROM ethereum.blocks ORDER BY "number" DESC LIMIT 1
    ),
    -- GRO directly withdrawn by contract owner (GRO vested not assigned to any wallet)
    team_withdrawn AS (
        SELECT SUM("amount") / 1e18 AS "amount"
        FROM gro_ethereum.GROTeamVesting_evt_LogWithdrawal
    ),
    -- GRO available (vested) not assigned to any wallet nor withdrawn yet
    team_available AS (
        SELECT u.amount - (v.total + w.amount) AS "amount"
        FROM team_vesting_totals v, team_withdrawn w, team_unlocked u
    ),
    -- GRO claimed (withdrawn by team member)
    team_claimed AS (
        SELECT SUM("amount") / 1e18 AS "amount"
        FROM gro_ethereum.GROTeamVesting_evt_LogClaimed
    ),
    /***********************************************************************************************
    **************************** I N V E S T O R S  -  GROInvVesting *******************************
    ***********************************************************************************************/
    investor_start_date AS (
        SELECT * FROM query_3407764
    ),
    investor_vests AS (
        SELECT vest."investor" as "investor",
               CAST(vest."amount" AS DOUBLE) / 1e18 as "amount"
        FROM gro_ethereum.GROInvVesting_evt_LogNewVest vest
    ),
    investor_vesting AS (
        SELECT iv."investor" as "investor",
               iv."amount" as "total_gro",
               CASE
                    WHEN TO_UNIXTIME(current_timestamp) < dates."start_date" + 31556952 -- start date + cliff
                        THEN 0
                    WHEN TO_UNIXTIME(current_timestamp) < dates."start_date" + 94670856 -- start date + vesting time
                        THEN iv."amount" * ( TO_UNIXTIME(current_timestamp) - dates."start_date" ) / 94670856
                    ELSE iv."amount"
               END as "vested_gro",
               CASE
                    WHEN TO_UNIXTIME(current_timestamp) < dates."start_date" + 31556952  -- start date + cliff
                        THEN iv."amount"
                    WHEN TO_UNIXTIME(current_timestamp) - 31556952 > dates."start_date"
                        THEN iv."amount" - iv."amount" * ( TO_UNIXTIME(current_timestamp) - dates."start_date" ) / 94670856
                    ELSE 0
               END as "vesting_gro"
    FROM investor_vests iv
        LEFT JOIN investor_start_date dates
        ON iv."investor" = dates."investor"
    ),
    investor_vesting_totals AS (
        SELECT
            SUM("total_gro") AS "total",
            SUM("vesting_gro") AS "vesting",
            SUM("vested_gro") AS "vested"
        FROM investor_vesting
    ),
    investor_claimed AS (
        SELECT SUM("amount") / 1e18 AS "amount"
        FROM gro_ethereum.GROInvVesting_evt_LogClaimed
    ),
    /***********************************************************************************************
    ************************************** T R E A S U R Y *****************************************
    ***********************************************************************************************/
    gro_liquid AS (
        SELECT
            SUM(
                CASE
                    WHEN ("to" = 0x359F4fe841f246a095a82cb26F5819E10a91fe0d
                     AND "from" = 0x0000000000000000000000000000000000000000) 
                    THEN CAST("value" AS DOUBLE)
                END
            ) / 1e18 AS treasury_amount_minted,
            SUM(
                CASE
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -CAST("value" AS DOUBLE)
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN CAST("value" AS DOUBLE)
                END
            ) / 1e18 AS total_amount
        FROM gro_ethereum.GROToken_evt_Transfer
    ),
    uni_gro_usdc_amount AS (
        SELECT
            SUM(
                CASE 
                    WHEN "from" = 0x0000000000000000000000000000000000000000 
                        AND "to" = 0x359F4fe841f246a095a82cb26F5819E10a91fe0d
                    THEN CAST("value" AS DOUBLE) / 1e18 
                    WHEN "from" = 0x359F4fe841f246a095a82cb26F5819E10a91fe0d 
                        AND "to" = 0x21c5918ccb42d20a2368bdca8feda0399ebfd2f6
                    THEN -CAST("value" AS DOUBLE) / 1e18 
                    ELSE 0 
                END) as treasury_position,
            SUM(
                CASE 
                    WHEN "from" = 0x0000000000000000000000000000000000000000 
                    THEN CAST("value" AS DOUBLE) / 1e18 
                    WHEN "to" = 0x21c5918ccb42d20a2368bdca8feda0399ebfd2f6
                    THEN -CAST("value" AS DOUBLE) / 1e18 
                    ELSE 0 
                END) as supply_amount
        FROM  uniswap_v2_ethereum.Pair_evt_Transfer
        WHERE "contract_address" = 0x21c5918ccb42d20a2368bdca8feda0399ebfd2f6
    ),
   uni_gro_udsc_reserve AS (
        SELECT
            CAST("reserve0" AS DOUBLE) / 1e18 as "amount",
            (CAST("reserve1" AS DOUBLE) / 1e6) / (CAST("reserve0" AS DOUBLE) / 1e18) as "gro_price"
        FROM  uniswap_v2_ethereum.Pair_evt_Sync
        WHERE "contract_address" = 0x21c5918ccb42d20a2368bdca8feda0399ebfd2f6
        ORDER BY "evt_block_number" DESC
        LIMIT 1
    ),
    /*
    community_total AS (
        SELECT total."total_amount" - total."treasury_amount_minted" - team_c."amount" - team_w."amount" - inv_c."amount" AS "amount"
        FROM gro_liquid total, team_claimed team_c, team_withdrawn team_w, investor_claimed inv_c
    ),
    */
    community_total AS (
        SELECT SUM("amount") AS "amount"
        FROM (
            SELECT
                CAST("unlocked" AS DOUBLE) / 1e18 AS "amount"
            FROM gro_ethereum.GROVesting_evt_LogExit
            UNION ALL
            SELECT
                CAST("unlocked" AS DOUBLE) / 1e18 AS "amount"
            FROM gro_ethereum.GROVestingV2_evt_LogExit
            UNION ALL
            SELECT
                CAST("mintingAmount" AS DOUBLE) / 1e18 AS "amount"
            FROM gro_ethereum.GROVestingV2_evt_LogInstantExit
        )
    ),
    /***********************************************************************************************
    **************************************** T O T A L S *******************************************
    ***********************************************************************************************/
    totals AS (
    -- Community
        SELECT 
            'Community' AS "type",
            45000000 AS "quota",
            45000000 / CAST(100000000 AS DOUBLE) AS "quota %",
            r."total" + liq."amount" AS "total",
            (r."total" + liq."amount") /  CAST(100000000 AS DOUBLE) AS "total %",
            (r."total" + liq."amount") * lp."gro_price" AS "total $",
            liq."amount" AS "liquid",
            liq."amount" * lp."gro_price" AS "liquid $",
            liq."amount" /  CAST(100000000 AS DOUBLE) AS "liquid %",
            r."total" AS "vesting/ed",
            r."total" / CAST(100000000 AS DOUBLE) AS "vesting/ed %",
            r."vesting" AS "vesting",
            r."vesting" / CAST(100000000 AS DOUBLE) AS "vesting %",
            r."vested" AS "vested",
            r."vested" / CAST(100000000 AS DOUBLE) AS "vested %",
            lp."gro_price" AS "gro_price"
        FROM rewards_vesting_totals r,
             community_total liq,
             uni_gro_udsc_reserve lp
        UNION ALL
        -- Investors
        SELECT
            'Investors' AS "type",
            19490577 AS "quota",
            19490577 / CAST(100000000 AS DOUBLE) AS "quota %",
            i."total" AS "total",
            i."total" /  CAST(100000000 AS DOUBLE) AS "total %",
            i."total" * lp."gro_price" AS "total $",
            liq."amount" AS "liquid",
            liq."amount" * lp."gro_price" AS "liquid $",
            liq."amount" /  CAST(100000000 AS DOUBLE) AS "liquid %",
            (i."total" -  liq."amount") AS "vesting/ed",
            (i."total" -  liq."amount") / CAST(100000000 AS DOUBLE) AS "vesting/ed %",
            i."vesting" AS "vesting",
            i."vesting" / CAST(100000000 AS DOUBLE) AS "vesting %",
            (i."vested" - liq."amount") AS "vested",
            (i."vested" - liq."amount") / CAST(100000000 AS DOUBLE) AS "vested %",
            lp."gro_price" AS "gro_price"
        FROM investor_vesting_totals i,
             investor_claimed liq,
             uni_gro_udsc_reserve lp
        UNION ALL
        -- Team
        SELECT
            'Team' AS "type",
            22509423 AS "quota",
            22509423 / CAST(100000000 AS DOUBLE) AS "quota %",
            t."total" + liq_w."amount" + a."amount" AS "total",
            (t."total" + liq_w."amount" + a."amount") /  CAST(100000000 AS DOUBLE) AS "total %",
            (t."total" + liq_w."amount" +  a."amount")  * lp."gro_price" AS "total $",
            liq.amount + liq_w."amount" AS "liquid",
            (liq.amount + liq_w."amount") * lp."gro_price" AS "liquid $",
            (liq.amount + liq_w."amount") /  CAST(100000000 AS DOUBLE) AS "liquid %",
            (t."total" +  a."amount" - liq."amount") AS "vesting/ed",
            (t."total" +  a."amount" - liq."amount") / CAST(100000000 AS DOUBLE) AS "vesting/ed %",
            t."vesting" AS "vesting",
            t."vesting" / CAST(100000000 AS DOUBLE) AS "vesting %",
            (t."vested" + a."amount" - liq."amount") AS "vested",
            (t."vested" + a."amount" - liq."amount") / CAST(100000000 AS DOUBLE) AS "vested %",
            lp."gro_price" AS "gro_price"
        FROM team_vesting_totals t,
             team_claimed liq,
             team_withdrawn liq_w,
             team_available a,
             uni_gro_udsc_reserve lp
        UNION ALL
        -- Treasury
        SELECT
            'Treasury' AS "type",
            13000000 AS "quota",
            13000000 / CAST(100000000 AS DOUBLE) AS "quota %",
            t."treasury_amount_minted" AS "total",
            t."treasury_amount_minted" /  CAST(100000000 AS DOUBLE) AS "total %",
            t."treasury_amount_minted" * lp."gro_price" AS "total $",
            t."treasury_amount_minted" AS "liquid",
            t."treasury_amount_minted" * lp."gro_price" AS "liquid $",
            t."treasury_amount_minted" /  CAST(100000000 AS DOUBLE) AS "liquid %",
            0 AS "vesting/ed",
            0 AS "vesting/ed %",
            0 AS "vesting",
            0 AS "vesting %",
            0 AS "vested",
            0 AS "vested %",
            lp."gro_price" AS "gro_price"
        FROM gro_liquid t,
             uni_gro_udsc_reserve lp
        ORDER BY 2 DESC
    ),
    grand_total AS (
        SELECT
            'Total' AS "type",
            SUM("quota") AS "quota",
            SUM("quota %") AS "quota %",
            SUM("total") AS "total",
            SUM("total %") AS "total %",
            SUM("total $") AS "total $",
            SUM("liquid") AS "liquid",
            SUM("liquid" * lp."gro_price") AS "liquid $",
            SUM("liquid %") AS "liquid %",
            SUM("vesting/ed") AS "vesting/ed",
            SUM("vesting/ed %") AS "vesting/ed %",
            SUM("vesting") AS "vesting",
            SUM("vesting %") AS "vesting %",
            SUM("vested") AS "vested",
            SUM("vested %") AS "vested %",
            lp."gro_price" AS "gro_price"
        FROM totals,
             uni_gro_udsc_reserve lp
        GROUP BY lp.gro_price
    )

SELECT * FROM totals
UNION ALL
SELECT * FROM grand_total
ORDER BY "quota" DESC


/*
-- Treasury was formerly calculated as:
-- 1) Current liquid GRO (iff GRO in & out)
-- 2) GRO position in UniswapV2's GRO/USDC pool

    gro_liquid AS (
        SELECT
            SUM(
                CASE
                    WHEN "to" = 0x359F4fe841f246a095a82cb26F5819E10a91fe0d THEN CAST("value" AS DOUBLE)
                    WHEN "from" = 0x359F4fe841f246a095a82cb26F5819E10a91fe0d THEN -CAST("value" AS DOUBLE)
                END
            ) / 1e18 AS treasury_amount,
            SUM(
                CASE
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -CAST("value" AS DOUBLE)
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN CAST("value" AS DOUBLE)
                END
            ) / 1e18 AS total_amount
        FROM gro_ethereum.GROToken_evt_Transfer
    ),
    uni_gro_usdc_total AS (
        SELECT (lp."treasury_position" / lp."supply_amount") * reserve."amount" AS "amount"
        FROM uni_gro_usdc_amount lp,
             uni_gro_udsc_reserve reserve
    ),
    treasury_totals AS (
        SELECT liq."treasury_amount" + lp."amount" AS "amount"
        FROM gro_liquid liq, uni_gro_usdc_total lp
    ),
*/