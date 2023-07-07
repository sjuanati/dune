WITH
    -- DAO voters
    voters AS (
        SELECT
            "voter" AS "user",
            count("voter") AS "num_votes"
        FROM snapshot.votes
        WHERE "space" = 'gro.xyz'
        AND "created" >= {{start timestamp}}
        GROUP BY "voter"
    ),
    proposals AS (
        SELECT count(*) as "proposals"
        FROM snapshot.proposals
        WHERE "space" = 'gro.xyz'
        AND "created" >= {{start timestamp}}
    ),
    voters_total AS (
        SELECT count(distinct("user")) as "voters"
        FROM voters
    ),
    -- GROVesting
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
    vesting_gro AS (
        SELECT
            "user" AS "user",
            "total_gro" AS "total_gro",
            CASE
                WHEN "startTime" + 31556952 > FLOOR(TO_UNIXTIME(current_timestamp))
                    THEN "total_gro" - "total_gro" * (FLOOR(TO_UNIXTIME(current_timestamp)) - "startTime") / (31556952)
                ELSE 0
            END as "vesting_gro",
            CASE
                WHEN "startTime" + 31556952 > FLOOR(TO_UNIXTIME(current_timestamp))
                    THEN "total_gro" * (FLOOR(TO_UNIXTIME(current_timestamp)) - "startTime") / (31556952)
                ELSE total_gro
            END as "vested_gro"
        FROM total_gro
    ),
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
            (0x04106fdd34485c03794f112e1c71ec6706bbb506, 0, 1641038400)
        ) AS t("contributor", "id", "start_date")
    ),
    team_vests AS (
        SELECT CAST(vest."id" AS INTEGER) as "id",
               vest."contributor" as "contributor",
               CAST(vest."amount" AS DOUBLE) / 1e18 as "amount"
        FROM gro_ethereum.GROTeamVesting_evt_LogNewVest vest
            LEFT JOIN gro_ethereum.GROTeamVesting_evt_LogStoppedVesting stop_vest
                 ON vest."contributor" = stop_vest."contributor"
                 AND vest."id" = stop_vest."id"
        WHERE stop_vest."contributor" IS NULL
        AND stop_vest."id" IS NULL
    ),
    team_vesting as (
         SELECT tv."contributor" as "user",
                tv."id" as "id",
                current_timestamp as "current_ts",
                dates."start_date" as "start_date",
                tv."amount" as "total_gro",
                TO_UNIXTIME(current_timestamp) as "now",
                CASE
                    WHEN TO_UNIXTIME(current_timestamp) < dates."start_date" + 31556952  -- start date + cliff
                        THEN tv.amount
                    WHEN TO_UNIXTIME(current_timestamp) - 31556952 > dates."start_date"
                        THEN tv."amount" - tv."amount" * ( TO_UNIXTIME(current_timestamp) - dates."start_date" ) / 94670856
                    ELSE 0
                END as "vesting_gro",
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
    team_vests_totals AS (
        SELECT
            "user",
            sum("total_gro") AS "total_gro",
            sum("vesting_gro") AS "vesting_gro",
            sum("vested_gro") AS "vested_gro"
        FROM team_vesting
        GROUP BY "user"
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
        SELECT vest."investor" as "investor",
               CAST(vest."amount" AS DOUBLE) / 1e18 as "amount"
        FROM gro_ethereum.GROInvVesting_evt_LogNewVest vest
    ),
    investor_vesting AS (
        SELECT iv."investor" as "user",
               iv."amount" as "total_gro",
               CASE
                    WHEN TO_UNIXTIME(current_timestamp) < dates."start_date" + 31556952  -- start date + cliff
                        THEN iv."amount"
                    WHEN TO_UNIXTIME(current_timestamp) - 31556952 > dates."start_date"
                        THEN iv."amount" - iv."amount" * ( TO_UNIXTIME(current_timestamp) - dates."start_date" ) / 94670856
                    ELSE 0
               END as "vesting_gro",
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
    liquid_gro AS (
        SELECT
            CASE WHEN "to" = 0x0000000000000000000000000000000000000000
                THEN "from"
                ELSE "to"
            END AS "user",
            CASE WHEN "to" = 0x0000000000000000000000000000000000000000 
                THEN -CAST(value AS DOUBLE) / 1e18
                ELSE CAST(value AS DOUBLE) / 1e18
            END AS "amount"
        FROM gro_ethereum.GROToken_evt_Transfer
        WHERE ("from" = 0x0000000000000000000000000000000000000000 OR "to" = 0x0000000000000000000000000000000000000000)
        UNION ALL
        SELECT
            CASE WHEN token_direction = 'from'
                THEN "from"
                ELSE "to"
            END AS "user",
            CASE WHEN token_direction = 'from'
                THEN -CAST(value AS DOUBLE) / 1e18 
                ELSE CAST(value AS DOUBLE) / 1e18
            END AS "amount"
        FROM gro_ethereum.GROToken_evt_Transfer
        CROSS JOIN (SELECT 'from' AS token_direction UNION ALL SELECT 'to') directions
        WHERE ("from" != 0x0000000000000000000000000000000000000000 AND "to" != 0x0000000000000000000000000000000000000000)
    ),
    liquid_gro_totals AS (
        SELECT
            "user",
            SUM(amount) AS "liquid_gro"
        FROM liquid_gro
        GROUP BY "user"
    ),
    totals AS (
        SELECT
            v."user",
            v."num_votes",
            COALESCE(liq.liquid_gro, 0) AS "liquid_gro",
            COALESCE(vest.total_gro, 0) + COALESCE(team.total_gro, 0) + COALESCE(inv.total_gro, 0) AS "total_gro",
            COALESCE(vest.vesting_gro, 0) + COALESCE(team.vesting_gro, 0) + COALESCE(inv.vesting_gro, 0) AS "vesting_gro",
            COALESCE(vest.vested_gro, 0) + COALESCE(team.vested_gro, 0) + COALESCE(inv.vested_gro, 0) AS "vested_gro"
        FROM voters v
            LEFT JOIN vesting_gro vest
                ON v.user = vest.user
            LEFT JOIN team_vests_totals team
                ON v.user = team.user
            LEFT JOIN investor_vesting inv
                ON v.user = inv.user
            LEFT JOIN liquid_gro_totals liq
                ON v.user = liq.user
    )

SELECT
    "user",
    "num_votes" AS "# votes",
    CASE WHEN "liquid_gro" < 0.0001 THEN 0 ELSE "liquid_gro" END AS "liquid gro",
    CASE WHEN "total_gro" < 0.0001 THEN 0 ELSE "total_gro" END AS "vested+ing gro",
    CASE WHEN "vested_gro" < 0.0001 THEN 0 ELSE "vested_gro" END AS "vested gro",
    CASE WHEN "vesting_gro" < 0.0001 THEN 0 ELSE "vesting_gro" END AS "vesting gro",
    CASE WHEN "total_gro" < 0.0001 THEN 0 ELSE "vesting_gro" / "total_gro" END AS "vesting %",
    voters,
    proposals
FROM totals, proposals, voters_total
ORDER BY "num_votes" DESC, "vesting_gro" DESC



/*
    voters AS (
    -- Manually extracted from Snapshot, from vote 16a to 26
    SELECT * FROM (VALUES
        (0x6eaf25B1B3670DFC17690ADfc001cC6dA65C2293, 9),
        (0xb7d8317800eF5f64DEB1a3BBEDcf08DF719C3CfA, 2),
        (0x50E27BB14dd13629b6ea3d7FeA2226bE9717760f, 3),
        (0x357dfdC34F93388059D2eb09996d80F233037cBa, 6),
        (0x540c44FF7B71F2487Cd11aaF53aeC1a85BFE8f7a, 7),
        (0x478cFEe0B09208f13Ef6fEc77f2a07307655541E, 2),
        (0xc103100D6278eA3838C9AF74ec5f90Ff43319A4E, 3),
        (0xa289364347bfC1912ab672425Abe593ec01Ca56E, 3),
        (0xbbe5C4c950f20da3F823d4376e8c7bc8e36B44c6, 2),
        (0xf7c7cA163DDFe69218377Ab7086bdFc9a50F4E1E, 2),
        (0x1ccb144B700EC726d37Db38c617E154De6d9c0d0, 10),
        (0x6c353732BF6e43CC4B6AFD73A793858E8074d47D, 2),
        (0x65cB74963eD540B271E9ab549f895E125793B68e, 3),
        (0x52d0Fc347560cC997f87984200F4a9344eCDC597, 13),
        (0x60ff7DcB4a9c1a89B18Fa2D1Bb9444143BbEA9BD, 9),
        (0x4501205A75F3e6a99f1320b802Fda91FB5499b11, 2),
        (0xfd4fd7e00f9Ebd266F3c9214A60Ecd90Fa9555f6, 1),
        (0x90e0d37f59B4d3202880d2FB17f3e50b7056f762, 3),
        (0x55248cb24b735372377fa8119c6Ab52bDa42fe19, 2),
        (0x5B191F5A2b4A867c4eD71858dacCc51FC59c69c0, 5),
        (0x16faa6e97AeC501Eb03D692eb5751e37519cDBb3, 5),
        (0x4a0E06B81EfD7aAf34aef567addeB5D794826C07, 2),
        (0x04106fdD34485c03794f112E1c71EC6706BbB506, 1),
        (0x69dF1F59A6C0FC50eE43eD5016d28f7CF3A6D69a, 5),
        (0x33107b56647014F37375b8291A86409f81A148Db, 2),
        (0x5EfeC2646eb5D768dfA12C9943eE2f1Cc458913b, 8),
        (0xeEBeCc6520F365Afb5FC5d934b269A0af2415a89, 7),
        (0x65331fa9893357A3c0459EAbb7866bAbd69C376F, 16),
        (0xb8fe77EE30ac42aeb913aD6bE67243fa9B241AE0, 13),
        (0xA8c4e3ce1743D0f2A6c227548C982a7C40569940, 14),
        (0x2ce1A66F22A2Dc6E410D9021D57aEB8D13D6bFEF, 7),
        (0x50017fC607DA965b6dd12f61C8a6560dFe2Df927, 6),
        (0x961cFe5b48D0020f9A0DeBF8124c043f6d12F823, 2),
        (0x5874affdA5b916E0470074C2c6be4d98de17849F, 2),
        (0xEc85B265c130AbBC0D7342fBC3585D5E0611b7E3, 14),
        (0x3772bFaD95Ea1E9171eD9Ce4Da3e2E2F3BbD099b, 13),
        (0xef0905745ce28eBe1deD7004146132fBfba548bA, 10),
        (0x78b8A76BEa31733777556033e2a116df66C4C41C, 13),
        (0x06517E034Ce7Bda386840cE9E5a02c3c53b47220, 11),
        (0x8e2F7D5aAAE5ABbD5052aCb74019b9b11cb74349, 14),
        (0x3Ec6732676dB7996c1b34E64b0503F941025Cb63, 4),
        (0x2F51A7f243cE11Ae52120D475A317E3568E9e65f, 4),
        (0xe7566A01c0Af00B90794b1DafAf7eEef23DE8678, 5),
        (0xeb3a4113907d38058955661faB0E81dD513761ED, 2),
        (0x10335BaedD7D204753d0098AfEe7C712ec550a90, 3),
        (0x38dAEa6f17E4308b0Da9647dB9ca6D84a3A7E195, 6),
        (0x186E20ae3530520C9F3E6C46F2f5d1062b784761, 3),
        (0x8A2F5d6D822611BDab08D306aA8F3E3942177417, 2),
        (0x0CC00f8752A3BA21629e639a3f01abfc9eF48c8c, 2),
        (0xAF1bff74708098dB603e48aaEbEC1BBAe03Dcf11, 2),
        (0x48f7D45FA696Dc89fF4f2233B25490455AE19DC2, 16),
        (0x2989301Ca9D09A233441905A895612FB62FA2760, 8),
        (0x8755491263b0F40318905CbE3594BA8Cd3F95189, 3),
        (0x91C224B419e8Da90c2C7e257D371172E7324a1F5, 6),
        (0x7CE06Dfb89aADEC277f34Fc575b3d735c593354E, 2),
        (0x79D6b9Ba9209785f7596022fc90ffA09D96BA5A1, 1),
        (0x2B19fDE5d7377b48BE50a5D0A78398a496e8B15C, 2),
        (0x6c8b8cC6963EE5dFC478b0213fc64eD9Eb103Fd9, 2),
        (0xfa49187F19edeEb7df7868Db82F2D723440B6C6E, 2),
        (0x95B9F2F528338b0cDB3F14442837b0e7F05DCEeC, 5),
        (0xeba06532a72bec44A1d33130aaa7C45c31e502F6, 2),
        (0x81aA6141923Ea42fCAa763d9857418224d9b025a, 2),
        (0x761e9c780896A849c8347851d190977fEF3aCa06, 3),
        (0x00ff6b7D26407A46Af2b631b4fa452a036d027E5, 12),
        (0x23B886AcEEb71458C96792ffd447352c277f820e, 2),
        (0x720b43Cb2AD865EAe6c0ADc23898FBf91A0B0A02, 1),
        (0x74b7E6d4336df8038A44393CAc5B7aC53fd75C7A, 2),
        (0xB6825B1d069824678dB46477D2a35d4B6445a66b, 1),
        (0x212d6d36440524b6A405D2B3984C15bBd3EBEFB3, 3),
        (0x551545C6aa92Cd6ED0E2fe9487008AC2bD91056A, 2),
        (0x7ee89D98B5731107A2A9CeBc3D9a4e729d070262, 1),
        (0x3eEFAa9d6e2ab7972C1001D41C82BB4881389257, 5),
        (0x6523F418c4b7260b070A17C47A206b66935D67cd, 5),
        (0x8A116d53D0Ec4bE2BF5fD8bC9BD606fA30be4224, 1),
        (0x8Cb6b6b3FE2A152dCc60a7b2d99C2BFDa6974e01, 5),
        (0x2536eE17BF914cd6D72ec9CBf9502dE9F1bbdFDF, 1),
        (0x6b31c05d4Fe7Bef12a61a2eE8360F883E2150590, 1),
        (0x7e3EfDA97FC0A5E892051c616828E3EF2F3F8843, 1),
        (0x92E216ac6Dcd7D67F138443f95fDc83bD7dd398f, 3),
        (0xD753c064B7570Df7E1A77d9ef2FceB1203a4aD1E, 1),
        (0xC6DDca4b715111c210608AEcDfA88cb4031b9690, 9),
        (0x4aECF1635A035a7cb8DB30BA12B80a4a95509268, 9),
        (0xC10898edA672fDFc4Ac0228bB1Da9b2bF54C768f, 1),
        (0x0CAd1d5ea8b4EeE26959cC00B4A3677f7A11e40F, 3),
        (0x7aC9C9135de57041095103d2aA8B5Be31B9aa415, 6),
        (0x7Bb9dCcc97052dDeF05141897fE8313fD4ad0418, 4),
        (0x453b33F33EAcfA5d8ad5BE61980F38e2A06AbbE3, 1),
        (0xbAA32387bd55553Ec806622d524b12BbB8242a19, 12),
        (0x7fCc3B4a05826c14afaFe6830F3511E9DDE48171, 3),
        (0xBD90F0243173E91385224a8117212d17C2E9e494, 13),
        (0x17395055B28bB32e33A3E2B8002C2a3fD1d41A24, 4),
        (0xb83Ac7549db7774aB59117483933bCF11f27C090, 3),
        (0xb3a7FBC2fa38C18ad4433aE93FE7F215Fd2D057f, 9),
        (0x27A11D3037BdAd382C1E15AC5e7e7D10e397f5bA, 4),
        (0xfA7D7352465883769a88B46A82183FC99D9E247c, 4),
        (0x85d3cb815548de9150d9fD42FF902c5c29Cc6036, 5),
        (0x360aBe51c7291a07a1b923258011eEe46EFFf32D, 1),
        (0x28A4F1C13fC3af4B4AC9CE8dAC5Eb7DE9dD35cA1, 1),
        (0x7B0B35b416631f05f321fE74990817dcA81caBDE, 6),
        (0xcB726F13479963934E91b6F34b6E87eC69c21bB9, 1),
        (0x2f1441652e993F17E6FEbB8023d2fD8f2c71f699, 3),
        (0x4F2769e87C7d96ED9CA72084845eE05e7dE5DdA2, 1),
        (0xea19157F846875ff2FCe465A55585ddbdee7b131, 2),
        (0xc54570f3751b79dd99231287C7036daA31e38888, 1),
        (0x11eBeE2bF244325B5559f0F583722d35659DDcE8, 4),
        (0x74aADAe2c7aA9321314A0A6F53a70eda5ebe103a, 3),
        (0x339Dab47bdD20b4c05950c4306821896CFB1Ff1A, 2),
        (0xcF39D265D6E7FeFF696295582d9D03f52bc36636, 3),
        (0xBB67bCe8D0E63082A5A349B35439F14C167E08F6, 6),
        (0x3a315f541003349aa597f6BfCf6DF6A2FeAb58E1, 4),
        (0x46808732DF838B1a9612829A1b33411E995c3e0C, 1),
        (0x89Fd8B7c8Ec147A35d872D0b436e950937C179E7, 1),
        (0x584313319d0bFeAd2a9d98a7154f5dCb5a62887F, 1),
        (0x8b391a1343b10b698C141637feaA3bB07C18586d, 1),
        (0x7195b3Eeb1a8B2B13895B4e2e5d2B694C5b30398, 2),
        (0x5a464E88d80648bEF4e4e741D5B202C2872EA566, 1),
        (0x173B6bC070eFdA94F5eE14dcb0FE05689C871f9F, 1),
        (0x9017c2af5e55E1Bc6B7E8B41bf732A8E8cBBAb0F, 1),
        (0x49c480ceAE7fE950296513A12512a3B25226B4B6, 1),
        (0x2067BEd542762D26E2755Ce7d8776728F3429f48, 2),
        (0x0c3F85AdF2436019Ff5920Ab56bCB9D27fFbef15, 2),
        (0x984253cAd96157B89255F4674E5362Fc0feb69Ee, 1),
        (0x5A2Cf5b6061De4Cd3C5936a781621fA8E4d1fDcF, 2),
        (0x3E64e0b69eb67E9aFd51E8B37f4bDaD2e3e2462b, 2),
        (0xDD172a224DcCa1DfC4FC13AE8e7E656922eA6F01, 1),
        (0x5246A99d5A9b7e1771c25D287BDDDFDB259189c4, 1),
        (0xf5045F18D619Ec631e2e07ac8d8Bf2d0282A1674, 1),
        (0x4969d896e585a9FF4CB4297D9630572d2f1B460A, 1),
        (0xa27b1b4651f52eF13566e9C026B3e0BaAb3b4156, 1),
        (0xdAE8c7af7930047561bD7357908080531A6EfE3f, 1),
        (0x50FEfB456648d7d2Ac43ea60De364E6Db4e793A3, 1),
        (0x22b908c2FeA7a1e6043FfcDBc77D660D4D326961, 2),
        (0x854F1269b659A727a2268AB86FF77CFB30BfB358, 1),
        (0x19A42d8444d22234336850F803961885B1972b64, 2),
        (0x4D73717Acea1C4aBf8EABa833088a3184dD806Dd, 1),
        (0xa7CA4568Be98778752Cecb9eD7698D7321a2CC71, 1),
        (0x476ECEe2dE6E667e0CD5a8CaFF49e9e8926c11Eb, 1),
        (0x96AC4a8925288b224972A007Ca91C99b46a02d1D, 1),
        (0x597D4Dc2b6662c37E42D9588754A40129704eACf, 1)
        ) AS t("user", "num_votes")
    ),
*/