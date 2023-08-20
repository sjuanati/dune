/*
/// @title Team vesting
/// @db_engine: v2 Dune SQL
/// @purpose Provide aggregated vesting figures for team
/// @data_validation: can't be done gobally, but per user basis:
///     total = GROTeamVesting(0xF43c6bDD2F9158B5A78DCcf732D190C490e28644).positionBalance()
///     vested = GROTeamVesting(0xF43c6bDD2F9158B5A78DCcf732D190C490e28644).positionVestedBalance()
///     vesting = emmm, no function() in contract, but the diff between total - vested ;)
*/

WITH
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
                WHEN stop_vest."contributor" IS NOT NULL
                    THEN CAST(stop_vest."unlocked" AS DOUBLE) / 1e18
                    ELSE CAST(vest."amount" AS DOUBLE) / 1e18
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
                dates."start_date" as "start_date",
                tv."amount" as "total_amount",
                CASE
                    WHEN TO_UNIXTIME(current_timestamp) < dates."start_date" + 31556952 -- start date + cliff
                        THEN 0
                    WHEN TO_UNIXTIME(current_timestamp) < dates."start_date" + 94670856 -- start date + vesting time
                        THEN tv."amount" * ( TO_UNIXTIME(current_timestamp) - dates."start_date" ) / 94670856
                    ELSE tv."amount"
                END as "vested",
                CASE
                    WHEN TO_UNIXTIME(current_timestamp) < dates."start_date" + 31556952  -- start date + cliff
                        THEN tv."amount"
                    WHEN TO_UNIXTIME(current_timestamp) - 31556952 > dates."start_date"
                        THEN tv."amount" - tv."amount" * ( TO_UNIXTIME(current_timestamp) - dates."start_date" ) / 94670856
                    ELSE 0
                END as "vesting"
        FROM team_vests tv
            LEFT JOIN team_start_date dates
            ON tv."contributor" = dates."contributor"
            AND tv."id" = dates."id"
    ),
    team_vesting_totals AS (
        SELECT
            "contributor" AS "contributor",
            "id" AS "position",
            "start_date" AS "startTS",
            from_unixtime("start_date") AS "startDate",
            SUM("total_amount") AS "total",
            SUM("vesting") AS "vesting",
            SUM("vested") AS "vested"
        FROM team_vesting
        GROUP BY 1,2,3
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
    team_vesting_agg AS (
        SELECT SUM("total_amount") AS "total" FROM team_vesting
    ),
    -- GRO available (vested) not assigned to any wallet nor withdrawn yet
    team_available AS (
        SELECT u.amount - (v.total + w.amount) AS "amount"
        FROM team_vesting_agg v, team_withdrawn w, team_unlocked u
    ),
    team_contract_owner AS (
        SELECT "newOwner" AS "owner"
        FROM gro_ethereum.GROTeamVesting_evt_OwnershipTransferred
        ORDER BY evt_block_number DESC
        LIMIT 1
    ),
    team_owner_totals AS (
        SELECT
            o."owner" AS "contributor",
            -1 AS "position",
            null AS "startTS",
            null AS "startDate",
            a."amount" AS "total",
            0 AS "vesting",
            a."amount" AS "vested"
        FROM team_contract_owner o, team_available a
    )

SELECT * FROM team_vesting_totals
UNION ALL
SELECT * FROM team_owner_totals
ORDER BY "total" DESC;
