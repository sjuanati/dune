/*
/// @title    GRO Token Distribution
/// @purpose  Understand the total GRO that could potentially be used towards governance
/// @engine   v2 Dune SQL
/// @Version  1.1
/// @kpi    - quota: GRO token allocation according to GRO Tokenomics
///         - total: liquid + vesting + vested GRO
///         - liquid: minted GRO in circulation
///         - vesting/ed: vesting + vested GRO
///         - vesting: locked GRO in GROVesting, GROTeamVesting & GROInvVesting contracts
///         - vested: unlocked GRO from above contracts excluding claims
///         - gro price: current dollar value of GRO based on Uniswap GRO/USDC pool
/// @dev    - Vested GRO for community deducts claims via LogExit, whereas vested GRO for team &
///           investors deducts claims via LogClaimed. LogInstantExit is excluded, as it represents
///           staking claims that can be vested or 30% claimed (but weren't vesting)
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
            "user" AS "user",
            CAST("amount" AS DOUBLE) / 1e18 as "amount",
            CAST(json_extract_scalar(json_parse("vesting"), '$.startTime') AS INTEGER) as startTime
        FROM gro_ethereum.GROVesting_evt_LogVest
        UNION ALL
        SELECT
            "user" AS "user",
            CAST("amount" AS DOUBLE) / 1e18 as "amount",
            CAST(json_extract_scalar(json_parse("vesting"), '$.startTime') AS INTEGER) as startTime
        FROM gro_ethereum.GROVestingV2_evt_LogVest
        UNION ALL
        SELECT
            "user" AS "user",
            -(CAST("unlocked" AS DOUBLE) / 1e18 + CAST("penalty" AS DOUBLE) / 1e18) as "amount",
            0 as startTime
        FROM gro_ethereum.GROVesting_evt_LogExit
        UNION ALL
        SELECT
            "user" AS "user",
            -(CAST("unlocked" AS DOUBLE) / 1e18 + CAST("penalty" AS DOUBLE) / 1e18) as "amount",
            0 as startTime
        FROM gro_ethereum.GROVestingV2_evt_LogExit
        UNION ALL
        SELECT
            "user" AS "user",
            0 as "amount",
            CAST("newPeriod" AS INTEGER) as "startTime"
        FROM gro_ethereum.GROVesting_evt_LogExtend
        UNION ALL
        SELECT
            "user" AS "user",
            0 as "amount",
            CAST("newPeriod" AS INTEGER) as "startTime"
        FROM gro_ethereum.GROVestingV2_evt_LogExtend
    ),
    total_gro AS (
        SELECT
            "user" AS "user",
            sum("amount") AS "total_gro",
            max("startTime") AS "startTime"
        FROM vests
        GROUP BY 1
    ),
    -- All positions were fully vested in September'23 to allow GRO token redemption
    vesting_gro AS (
        SELECT
        "user" AS "user",
        "total_gro" AS "total_gro",
        --CASE
        --    WHEN "startTime" + 31556952 > FLOOR(TO_UNIXTIME(current_timestamp))
        --        THEN "total_gro" - "total_gro" * (FLOOR(TO_UNIXTIME(current_timestamp)) - "startTime") / (31556952)
        --    ELSE 0
        --END AS "vesting_gro",
        0 AS "vesting_gro",
        --CASE
        --    WHEN "startTime" + 31556952 > FLOOR(TO_UNIXTIME(current_timestamp))
        --        THEN "total_gro" * (FLOOR(TO_UNIXTIME(current_timestamp)) - "startTime") / (31556952)
        --    ELSE total_gro
        --END AS "vested_gro"
        "total_gro" AS "vested_gro"
        FROM total_gro
    ),
    rewards_vesting_totals AS (
        SELECT
            SUM("total_gro") AS "total",
            SUM("vesting_gro") AS "vesting",
            SUM("vested_gro") AS "vested"
        FROM vesting_gro
    ),
    /***********************************************************************************************
    ********************************* T E A M  -  GROTeamVesting ***********************************
    ***********************************************************************************************/
    team_start_date AS (
      SELECT * 
      FROM (VALUES 
        (0xfa5e54667bf2e3536ee386672b57809ee182d979, 0, 1632844800),
        (0xf59cc73cc03b0366c53f41144691fbd6f9027801, 0, 1632844800),
        (0xf23b0e575ca65f26ba40eae794b9a8b903715cb7, 0, 1641250800),
        (0xd332cb987c22edf44e35cd2128dd6b4ffc1437b8, 0, 1632844800),
        (0xd0ec53a6144dee637052bf94b443fd1d49f45076, 0, 1632844800),
        (0xbeb749038a286733f4f486a7ef93a0b9fa11970d, 0, 1632844800),
        (0xaaace175d9cdf31cda7b2c814ea79ca735042289, 0, 1632844800),
        (0xa8add9437ce01cd99465045a2d8d87076c8367c6, 0, 1632844800),
        (0xa31f8afd785ec32df8df77ab83978e49cc0349ac, 1, 1661990400),
        (0xa31f8afd785ec32df8df77ab83978e49cc0349ac, 0, 1632844800),
        (0x9bf1728b081ebe2e5468d0ed5030b6f65e92ae21, 0, 1632844800),
        (0x972b27f38d42345b0c491cf8e7737b880bc96275, 2, 1661990400),
        (0x972b27f38d42345b0c491cf8e7737b880bc96275, 0, 1641038400),
        (0x91f8c5d3e33c61b0662d439d16fe576c012381d5, 0, 1648425600),
        (0x8b391a1343b10b698c141637feaa3bb07c18586d, 0, 1632844800),
        (0x86775e8542a30aa7f6e9c5a10b60999aee6bf0e9, 0, 1660604400),
        (0x859df1b9bb101715b7c9bfc213378e383d216241, 1, 1661990400),
        (0x849dcd4b658adc5723f8eb77364eb322175de3fd, 0, 1632844800),
        (0x843c86947e7c2b8461ec22960ea8ea05ee0be294, 0, 1632844800),
        (0x6aff656a5e44c3e9af2bc19cccafe7d30cd4cd09, 0, 1658876400),
        (0x40fcdd8c164ea13cfd85a871e8755e977d885da4, 0, 1662048000),
        (0x37fcd10a075e67bb6e092201c930a896d59a42f4, 0, 1648940400),
        (0x37ef4f2a90c9806cd8f3f20e203ef1f2e265ef1f, 0, 1656543600),
        (0x106e7eca4a0dac78eadfab1fea20336290694139, 0, 1632844800),
        (0x106e7eca4a0dac78eadfab1fea20336290694139, 1, 1647259200),
        (0x08d0b7efd89319c2baddab8dd5ba8c7952aedcd4, 0, 1649635200),
        (0x04106fdd34485c03794f112e1c71ec6706bbb506, 2, 1661990400),
        (0x04106fdd34485c03794f112e1c71ec6706bbb506, 0, 1641038400),
        (0x95c1d2014909c04202fa73820b894b45f054f25e, 0, 1680498000)
      ) AS t("contributor", "id", "start_date")
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
                        THEN tv."amount" * ( TO_UNIXTIME(current_timestamp) - dates."start_date" ) / 94670856
                    ELSE tv."amount"
                END as "vested_gro",
                CASE
                    WHEN TO_UNIXTIME(current_timestamp) < dates."start_date" + 31556952  -- start date + cliff
                        THEN tv.amount
                    WHEN TO_UNIXTIME(current_timestamp) - 31556952 > dates."start_date"
                        THEN tv."amount" - tv."amount" * ( TO_UNIXTIME(current_timestamp) - dates."start_date" ) / 94670856
                    ELSE 0
                END as "vesting_gro"
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
    community_total AS (
        SELECT total."total_amount" - total."treasury_amount_minted" - team_c."amount" - team_w."amount" - inv_c."amount" AS "amount"
        FROM gro_liquid total, team_claimed team_c, team_withdrawn team_w, investor_claimed inv_c
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