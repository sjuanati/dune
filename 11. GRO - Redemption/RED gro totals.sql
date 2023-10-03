WITH
    gro_liquid AS (
        SELECT SUM(amount) AS "amount" 
        FROM (
            SELECT
                CASE
                    WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -CAST("value" AS DOUBLE)
                    WHEN "from" = 0x0000000000000000000000000000000000000000 THEN CAST("value" AS DOUBLE)
                END / 1e18 AS amount
            FROM gro_ethereum.GROToken_evt_Transfer
        )
    ),
    gro_price AS (
        SELECT (CAST(reserve1 AS DOUBLE) / 1e6) / (CAST(reserve0 AS DOUBLE) / 1e18) as "value"
        FROM  uniswap_v2_ethereum.Pair_evt_Sync
        WHERE contract_address = 0x21c5918ccb42d20a2368bdca8feda0399ebfd2f6
        ORDER BY evt_block_number DESC
        LIMIT 1
    ),
    /***********************************************************************************************
    ****************************** C O M M U N I T Y  -  GROVesting ********************************
    ***********************************************************************************************/
    vested AS (
        SELECT CAST("amount" AS DOUBLE) / 1e18 AS "amount"
        FROM gro_ethereum.GROVesting_evt_LogVest
        UNION ALL
        SELECT CAST("amount" AS DOUBLE) / 1e18 AS "amount"
        FROM gro_ethereum.GROVestingV2_evt_LogVest
        UNION ALL
        SELECT -CAST("unlocked" AS DOUBLE) / 1e18 - CAST("penalty" AS DOUBLE) / 1e18 AS "amount"
        FROM gro_ethereum.GROVesting_evt_LogExit
        UNION ALL
        SELECT -CAST("unlocked" AS DOUBLE) / 1e18 - CAST("penalty" AS DOUBLE) / 1e18 AS "amount"
        FROM gro_ethereum.GROVestingV2_evt_LogExit
        UNION ALL
        SELECT -CAST("mintingAmount" AS DOUBLE) / 1e18 - CAST("penalty" AS DOUBLE) / 1e18 AS "amount"
        FROM gro_ethereum.GROVestingV2_evt_LogInstantExit
    ),
    community_vested AS (
        SELECT sum("amount") AS "amount" FROM vested
    ),
    /***********************************************************************************************
    ********************************* T E A M  -  GROTeamVesting ***********************************
    ***********************************************************************************************/
    -- GRO in team vesting contract already vested but not yet claimed (minted)
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
                END as "vested_gro"
        FROM team_vests tv
            LEFT JOIN team_start_date dates
            ON tv."contributor" = dates."contributor"
            AND tv."id" = dates."id"
    ),
    team_vesting_totals AS (
        SELECT
            SUM("total_gro") AS "total",
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
    team_vested AS (
        SELECT
            (t."vested" + a."amount" - liq."amount") AS "amount"
        FROM team_vesting_totals t,
             team_claimed liq,
             team_available a
    ),
    /***********************************************************************************************
    **************************** I N V E S T O R S  -  GROInvVesting *******************************
    ***********************************************************************************************/
    -- GRO in investor vesting contract already vested but not yet claimed (minted)
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
               END as "vested_gro"
    FROM investor_vests iv
        LEFT JOIN investor_start_date dates
        ON iv."investor" = dates."investor"
    ),
    investor_vesting_totals AS (
        SELECT
            SUM("total_gro") AS "total",
            SUM("vested_gro") AS "vested"
        FROM investor_vesting
    ),
    investor_claimed AS (
        SELECT SUM("amount") / 1e18 AS "amount"
        FROM gro_ethereum.GROInvVesting_evt_LogClaimed
    ),
    investor_vested AS (
        SELECT (i."vested" - liq."amount") AS "amount"
        FROM investor_vesting_totals i,
             investor_claimed liq
    )

SELECT
    gl."amount" AS "gro_liquid",
    tv."amount" AS "gro_team_vested",
    iv."amount" AS "gto_investor_vested",
    cv."amount" AS "gro_community_vested",
    gl."amount" + tv."amount" + iv."amount" + cv."amount" AS "total_gro_redeemable",
    p."value"
FROM
    gro_liquid gl,
    gro_price p,
    team_vested tv,
    investor_vested iv,
    community_vested cv
