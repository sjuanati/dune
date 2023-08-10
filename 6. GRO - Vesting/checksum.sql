/*
/// @title Vesting checksum
/// @db_engine: v2 Dune SQL
/// @purpose Detect team or investor wallets that do not have a startDate in the Dashboard yet
/// @dev: startDate is not emmited through any event, so it must be manually added in the Dashboard queries
///       when a new vesting position is created:
///     - GROTeamVesting contract: 0xF43c6bDD2F9158B5A78DCcf732D190C490e28644, function: contributorPositions()
///     - GROInvVesting contract: 0x0537d3DA1Ed1dd7350fF1f3B92b727dfdBAB80f1, function: investorPositions()
/// @dev: Queries that require update if a new position is created
///     - GRO - Totals -> https://dune.com/queries/2600715
///     - GRO - GROInvestorVesting -> https://dune.com/queries/2513706
///     - GRO - GROTeamVesting -> https://dune.com/queries/2513704
*/

WITH
    -- GROTeamVesting
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
        SELECT CAST(vest."id" AS INTEGER) as "id",
               vest."contributor" as "contributor"
        FROM gro_ethereum.GROTeamVesting_evt_LogNewVest vest
            LEFT JOIN gro_ethereum.GROTeamVesting_evt_LogStoppedVesting stop_vest
                 ON vest."contributor" = stop_vest."contributor"
                 AND vest."id" = stop_vest."id"
        WHERE stop_vest."contributor" IS NULL
        AND stop_vest."id" IS NULL
    ),
    -- GROInvVesting
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
        SELECT "investor" AS "investor"
        FROM gro_ethereum.GROInvVesting_evt_LogNewVest
    )

SELECT
    i.investor AS "wallet",
    NULL AS "position",
    'investor' AS "type"
FROM investor_vests i
    LEFT JOIN investor_start_date i_dates
    ON i."investor" = i_dates."investor"
WHERE i_dates."investor" IS NULL
UNION ALL
SELECT
    t."contributor" AS "wallet",
    t."id" AS "position",
    'team' AS "type"
FROM team_vests t
    LEFT JOIN team_start_date t_dates
    ON t."contributor" = t_dates."contributor"
WHERE t_dates."contributor" IS NULL
