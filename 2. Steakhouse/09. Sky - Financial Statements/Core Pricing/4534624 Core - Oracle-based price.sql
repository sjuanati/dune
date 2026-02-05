/*
@title: sUSDS Overview multichain
@author: Steakhouse Financial
@description: Calculates price feeds based on oracles
@Scope:
    Chainlink oracle: EURCV, USCC, USTB, wbIB01
    MIDAS oracle: mF-ONE, mBASIS, mRe7YIELD, mEDGE, mMEV
    Redstone oracle: STAC
    SSR Oracle: sUSDS (Ethereum, Base), sUSDC (Ethereum, Base)
@dev: when adding new price feeds, remember updating matview query_4583090
@version:
    - 1.0 - 2025-01-07 - Initial version
    - 2.0 - 2025-04-24 - Extended SSR price feed to sUSDS and sUSDC in Ethereum, Base, Arbitrum
                         (sUSDC is a 4626 vault that deposits into sUSDS under the hood)
                       - Refactored chainlink query
    - 3.0 - 2025-05-02 - Added EUR<>USD chainlink aggregators to calculate usd price for all eur-stablecoins
    - 4.0 - 2025-06-24 - Add Midas mF-One
    - 5.0 - 2025-08-13 - Added Ondo's OUSG
    - 6.0 - 2025-08-21 - Added DeFi Janus Henderson Anemoy AAA CLO Fund Token (deJAAA)
    - 7.0 - 2025-10-02 - Add new tokens for sUSDS for Unichain, Base, Optimism, and Arbitrum
    - 8.0 - 2026-01-09 - Add redstone oracle for STAC
*/

with
    tokens(blockchain, block_num, token_address, oracle_type, oracle_address, oracle_decimals, symbol) as (
    values
        -- chainlink : EUR<>USD aggregators
        ('ethereum', 16739369, null, 'chainlink', 0x25Fa978ea1a7dc9bDc33a2959B9053EaE57169B5, 8, 'EUR'),
        ('ethereum', 16739369, null, 'chainlink', 0x7D7C4A33D044798443D49037a17B1bad44310392, 8, 'EUR'),
        ('ethereum', 16739369, null, 'chainlink', 0x8f71c9c583248A11CAcBbC8FD0D5dFa483D3b109, 8, 'EUR'),
        ('ethereum', 16739369, null, 'chainlink', 0x02F878A94a1AE1B15705aCD65b5519A46fe3517e, 8, 'EUR'),
        ('ethereum', 16739369, null, 'chainlink', 0x966Dad3B93C207A9EE3a79C336145e013C5cD3fc, 8, 'EUR'),
        -- chainlink : Other aggregators
        ('ethereum', 20188111, 0xCA30c93B02514f86d5C86a6e375E3A330B435Fb5, 'chainlink', 0xE6c7AE04e83aa7e491988cAeecf5BD6a240A0d14, 8, 'bIB01'),
        ('ethereum', 20188111, 0xCA30c93B02514f86d5C86a6e375E3A330B435Fb5, 'chainlink', 0x788D911ae7c95121A89A0f0306db65D87422E1de, 8, 'bIB01'),
        ('ethereum', 20188111, 0xCA30c93B02514f86d5C86a6e375E3A330B435Fb5, 'chainlink', 0x5EE6Ee50c1cB3E8Da20eE83D57818184387433e8, 8, 'bIB01'),
        ('ethereum', 16739369, 0x52d134c6db5889fad3542a09eaf7aa90c0fdf9e4, 'chainlink', 0x5f8c943a29FFfC7Df8cE4001Cf1bedbCFC610476, 8, 'bIBTA'),
        ('ethereum', 16739369, 0x52d134c6db5889fad3542a09eaf7aa90c0fdf9e4, 'chainlink', 0x9f9953D8A2C5366f098754D48F2d69b144cE03Da, 8, 'bIBTA'),
        ('ethereum', 17371182, 0x20C64dEE8FdA5269A78f2D5BDBa861CA1d83DF7a, 'chainlink', 0x9E8E794ad6Ecdb6d5c7eaBE059D30E907F58859b, 8, 'bHIGH'),
        ('ethereum', 17485361, 0x2f123cf3f37ce3328cc9b5b8415f9ec5109b45e7, 'chainlink', 0x83Ec02059F686E747392A22ddfED7833bA0d7cE3, 8, 'bC3M'),
        ('ethereum', 17485361, 0x3f95aa88ddbb7d9d484aa3d482bf0a80009c52c9, 'chainlink', 0x475855DAe09af1e3f2d380d766b9E630926ad3CE, 8, 'bERNX'),
        ('ethereum', 18725909, 0x14d60e7fdc0d71d8611742720e4c50e7a974020c, 'chainlink', 0x5C00518D3d423EC59D553Af123Be8a63B11078CF, 6, 'USCC'),
        ('ethereum', 20270220, 0x43415eB6ff9DB7E26A15b704e7A3eDCe97d31C4e, 'chainlink', 0xd5BC4e3c7e77A5776fD9D0DDe8471B8B4aEc10f5, 6, 'USTB'),
        -- spark : ssr oracle
        ('base', 20891577, 0x5875eEE11Cf8398102FdAd704C9E96607675467a, 'ssr', 0x, 18, 'sUSDS'),
        ('base', 27123520, 0x3128a0F7f0ea68E7B7c9B00AFa7E41045828e858, 'ssr', 0x, 18, 'sUSDC'),
        ('ethereum', 20677434, 0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD, 'ssr', 0x, 18, 'sUSDS'),
        ('ethereum', 21969024, 0xBc65ad17c5C0a2A4D159fa5a503f4992c7B545FE, 'ssr', 0x, 18, 'sUSDC'),
        ('arbitrum', 300480142, 0xdDb46999F8891663a8F2828d25298f70416d7610, 'ssr', 0x, 18, 'sUSDS'),
        ('arbitrum', 311940473, 0x940098b108fB7D0a7E374f6eDED7760787464609, 'ssr', 0x, 18, 'sUSDC'),
        ('unichain', 15298547, 0xa06b10db9f390990364a3984c04fadf1c13691b5, 'ssr', 0x, 18, 'sUSDS'),
        ('unichain', 15298547, 0x14d9143BEcC348920b68D123687045db49a016C6, 'ssr', 0x, 18, 'sUSDS'),
        ('optimism', 135223366, 0xb5b2dc7fd34c249f4be7fb1fcea07950784229e0, 'ssr', 0x, 18, 'sUSDS'),
        ('optimism', 135223366, 0xCF9326e24EBfFBEF22ce1050007A43A3c0B6DB55, 'ssr', 0x, 18, 'sUSDC')
    ),
    -- ************************************************************************************************************************
    -- *********************                            C H A I N L I N K                                 *********************
    -- ************************************************************************************************************************
    -- The following cte replaces backed query https://dune.com/queries/2474657 
    chainlink_pricing as (
        select
            date(evt_block_time) as dt,
            blockchain,
            symbol,
            token_address,
            max_by("current" * pow(10, -oracle_decimals), evt_block_time) as price_usd
        from (
            select t.blockchain, t.token_address, t.symbol, t.oracle_decimals, a.evt_block_time, a."current"
            from chainlink_ethereum.Aggregator_evt_AnswerUpdated a
            join tokens t on a.contract_address = t.oracle_address
            where a.evt_block_number >= t.block_num
              and t.oracle_type = 'chainlink'
            union all
            select t.blockchain, t.token_address, t.symbol, t.oracle_decimals, a.evt_block_time, a."current"
            from chainlink_ethereum.AccessControlledOCR2Aggregator_evt_AnswerUpdated a
            join tokens t on a.contract_address = t.oracle_address
            where a.evt_block_number >= t.block_num
              and t.oracle_type = 'chainlink'
            union all
            select t.blockchain, t.token_address, t.symbol, t.oracle_decimals, a.evt_block_time, a."current"
            from backed_ethereum.backedoracle_evt_answerupdated a
            join tokens t on a.contract_address = t.oracle_address
            where a.evt_block_number >= t.block_num
              and t.oracle_type = 'chainlink'
            union all
            select t.blockchain, t.token_address, t.symbol, t.oracle_decimals, a.evt_block_time, a."current"
            from chainlink_ethereum.accesscontrolledoffchainaggregator_evt_answerupdated a
            join tokens t on a.contract_address = t.oracle_address
            where a.evt_block_number >= t.block_num
              and t.oracle_type = 'chainlink'
        )
        group by 1, 2, 3, 4
        union all
        -- @TODO: not up-to-date; probably to be changed
        select
            block_date as dt,
            blockchain,
            'wbIB01' as symbol,
            0xcA2A7068e551d5C4482eb34880b194E4b945712F as token_address,
            max_by(underlying_token_price, block_time) as price_usd
        from chainlink.price_feeds
        where blockchain = 'ethereum'
          and feed_name like 'IB01%'
          and block_time >= date '2023-06-02'
        group by 1,2,3,4
    ),
    -- ************************************************************************************************************************
    -- *********************                          S P A R K' S  S.S.R                                 *********************
    -- ************************************************************************************************************************
    -- retrieve parameter changes in SSR Oracle
    ssr_oracle as (
        select
            date(evt_block_time) as dt,
            1 as ray,
            max_by(cast(json_extract_scalar(json_parse(nextData), '$.ssr') as uint256) / 1e27, evt_block_time) as ssr,
            max_by(cast(json_extract_scalar(json_parse(nextData), '$.chi') as uint256) / 1e27, evt_block_time) as chi,
            max_by(cast(json_extract_scalar(json_parse(nextData), '$.rho') as uint256), evt_block_time) as rho
        from sky_multichain.ssrauthoracle_evt_setsusdsdata
        group by 1
    ),
    -- create daily sequence and backfill oracle params
    ssr_params_seq as (
        select
            dt,
            to_unixtime(dt) as ts,
            1 as ray,
            coalesce(ssr, last_value(ssr) ignore nulls over (order by dt rows between unbounded preceding and current row)) as ssr,
            coalesce(chi, last_value(chi) ignore nulls over (order by dt rows between unbounded preceding and current row)) as chi,
            coalesce(rho, last_value(rho) ignore nulls over (order by dt rows between unbounded preceding and current row)) as rho
        from (
            select *
            from unnest(sequence(date '2024-10-02', current_date, interval '1' day)) as t(dt)
            left join ssr_oracle using(dt)
        )
    ),
    -- precalcs for the pricing
    ssr_base as (
        select
            dt,
            ray,
            chi,
            (ssr - ray) as rate,
            (ts - rho) as exp
        from ssr_params_seq
    ),
    -- calculate pricing based on binomial approximation, as described in:
    -- https://basescan.org/address/0x65d946e533748A998B1f0E430803e39A6388f7a1#code#F3#L66
    ssr_price_estimation as (
        select
            dt,
            chi * (ray + rate * exp + secondTerm + thirdTerm) / ray as price_usd
        from (
            select
                dt,
                ray as ray,
                chi as chi,
                exp,
                rate,
                exp * (exp - 1) * (power(rate, 2) / ray) / 2 as secondTerm,
                exp * (exp - 1) * (exp - 2) * (power(rate, 3) / power(ray,2)) / ray as thirdTerm
            from ssr_base
        )
    ),
    -- apply SSR oracle price to sUSDS & sUSDC in all chains
    ssr_pricing as (
        select
            s.dt,
            t.blockchain,
            t.symbol,
            t.token_address,
            s.price_usd
        from ssr_price_estimation s
        cross join tokens t
        where t.oracle_type = 'ssr'
    ),
    -- ************************************************************************************************************************
    -- ***************************                          O U S D                                 ***************************
    -- ************************************************************************************************************************
    ousd_pricing as (
        select
            date(ts) as dt,
            'ethereum' as blockchain,
            'OUSG' as symbol,
            0x1b19c19393e2d034d8ff31ff34c81252fcbbee92 as token_address,
            max_by(price_usd / 1e18, ts) as price_usd
        from (
            select
                evt_block_time as ts,
                newRWAPrice as price_usd
            from fluxfinance_ethereum.rwaoracleexternalcomparisoncheck_evt_rwaexternalcomparisoncheckpriceset
            union all
            select
                evt_block_time as ts,
                newPrice as price_usd
            from fluxfinance_ethereum.rwaoracleratecheck_evt_rwapriceset
            union all
            select
                evt_block_time as ts,
                newPrice as price_usd
            from fluxfinance_ethereum.OndoPriceOracleV2_evt_UnderlyingPriceSet
        )
        group by 1
    )
    -- ************************************************************************************************************************
    -- ***************************               M I D A S + REDSTONE                               ***************************
    -- ************************************************************************************************************************
    , midas_tokens(symbol, start_dt, token_address, oracle_address, oracle_decimals) as (
        VALUES
        ('mF-ONE', TIMESTAMP'2025-05-27 00:00', 0x238a700eD6165261Cf8b2e544ba797BC11e466Ba, 0x8D51DBC85cEef637c97D02bdaAbb5E274850e68C, 8),
        ('mTBILL', TIMESTAMP'2024-08-21 00:00', 0xDD629E5241CbC5919847783e6C96B2De4754e438, 0x056339C044055819E8Db84E71f5f2E1F536b2E5b, 8),
        ('mRe7YIELD', TIMESTAMP'2025-02-06 00:00', 0x87C9053C819bB28e0D73d33059E1b3DA80AFb0cf, 0x0a2a51f2f206447dE3E3a80FCf92240244722395, 8),
        ('mBASIS', TIMESTAMP'2024-08-21 00:00', 0x2a8c22E3b10036f3AEF5875d04f8441d4188b656, 0xE4f2AE539442e1D3Fb40F03ceEbF4A372a390d24, 8),
        ('mEDGE', TIMESTAMP'2025-01-21 00:00', 0xbB51E2a15A9158EBE2b0Ceb8678511e063AB7a55, 0x698dA5D987a71b68EbF30C1555cfd38F190406b7, 8),
        ('mMEV', TIMESTAMP'2025-02-04 00:00', 0x030b69280892c888670EDCDCD8B69Fd8026A0BF3, 0x5f09Aff8B9b1f488B7d1bbaD4D89648579e55d61, 8)
    
    )
    , redstone_tokens(symbol, start_dt, token_address, feed_id, oracle_decimals) as (
        VALUES
        ('STAC', TIMESTAMP'2025-11-05 00:00', 0x51C2d74017390CbBd30550179A16A1c28F7210fc, 0x535441435f46554e44414d454e54414c00000000000000000000000000000000, 8)
    )
    , mid_stone_tokens as (
        SELECT symbol, start_dt, token_address, oracle_decimals FROM midas_tokens
        UNION ALL
        SELECT symbol, start_dt, token_address, oracle_decimals FROM redstone_tokens
        
    )
    , mid_stone_oracle_events as (
        SELECT token_address
            , ts as last_ts
            , price as last_price
            , LAG(ts) IGNORE NULLS OVER (PARTITION BY token_address ORDER BY ts) as last_last_ts
            , LAG(price) IGNORE NULLS OVER (PARTITION BY token_address ORDER BY ts) as last_last_price
            , COALESCE(lead(ts) IGNORE NULLS OVER (PARTITION BY token_address ORDER BY ts), NOW()) as next_ts
            , LEAD(price) IGNORE NULLS OVER (PARTITION BY token_address ORDER BY ts) as next_price
            , LEAD(ts, 2) IGNORE NULLS OVER (PARTITION BY token_address ORDER BY TS) as next_next_ts
            , LEAD(price, 2) IGNORE NULLS OVER (PARTITION BY token_address ORDER BY TS) as next_next_price
        FROM (
            SELECT token_address, data / POWER(10, oracle_decimals) as price
                , from_unixtime(timestamp) as ts
            FROM midas_rwa_ethereum.mfonecustomaggregatorfeed_evt_answerupdated as o
                JOIN midas_tokens as t ON o.contract_address = t.oracle_address
            UNION ALL
            SELECT token_address, data / POWER(10, oracle_decimals) as price
                , from_unixtime(timestamp) as ts
            FROM midas_rwa_ethereum.mtbillcustomaggregatorfeed_evt_answerupdated as o
                JOIN midas_tokens as t ON o.contract_address = t.oracle_address
            UNION ALL
            SELECT token_address, data / POWER(10, oracle_decimals) as price
                , from_unixtime(timestamp) as ts
            FROM midas_rwa_ethereum.mbasiscustomaggregatorfeed_evt_answerupdated as o
                JOIN midas_tokens as t ON o.contract_address = t.oracle_address
            UNION ALL
            SELECT token_address, data / POWER(10, oracle_decimals) as price
                , from_unixtime(timestamp) as ts
            FROM mmev_ethereum.mmevcustomaggregatorfeed_evt_answerupdated as o
                JOIN midas_tokens as t ON o.contract_address = t.oracle_address
            UNION ALL
            SELECT token_address, data / POWER(10, oracle_decimals) as price
                , from_unixtime(timestamp) as ts
            FROM midas_rwa_ethereum.customaggregatorfeed_evt_answerupdated as o
                JOIN midas_tokens as t ON o.contract_address = t.oracle_address
            UNION ALL
            SELECT token_address, value / POWER(10, oracle_decimals) as price
                , from_unixtime(updatedAt) as ts
            FROM redstone_ethereum.ethereummultifeedadapterwithoutroundsv1_evt_valueupdate as o
                JOIN redstone_tokens as t ON o.dataFeedId = t.feed_id
        )
    )
    , mid_stone_series as (
        select dt, symbol, token_address
        from mid_stone_tokens, unnest(sequence(mid_stone_tokens.start_dt, current_date, interval '1' day)) as s(dt)
    )
    , mid_stone_pricing as (
        SELECT s.dt
            , 'ethereum' as blockchain
            , s.symbol
            , s.token_address
            , CASE
                WHEN last_price is not null and next_price is not null
                    THEN last_price + (next_price - last_price)/date_diff('second', last_ts, next_ts) * (date_diff('second', last_ts, dt))
                WHEN next_price is NOT null and next_next_price is not null
                    THEN next_price - (next_next_price - next_price)/date_diff('second', next_next_ts, next_ts) * (date_diff('second', dt, next_ts))
                WHEN last_price is NOT null and last_last_price is not null 
                    THEN last_price + ((last_price - last_last_price) / date_diff('second', last_last_ts, last_ts)) * (date_diff('second', last_ts, dt))
                END as price_usd
        FROM mid_stone_series as s
            JOIN mid_stone_oracle_events as moe on s.token_address = moe.token_address and s.dt between moe.last_ts and moe.next_ts 
    )
    , dejaaa as (
        select
            evt_block_date as dt,
            chain as blockchain,
            'deJAAA' as symbol,
            0xAAA0008C8CF3A7Dca931adaF04336A5D808C82Cc as token_address,
            max_by(price / 1e18, evt_block_time) as price_usd
        from centrifuge_multichain.spoke_evt_updateshareprice
        where poolId = 281474976710659
          and scId = 0x00010000000000030000000000000001
        group by 1,2
    ),
    prices as (
        select * from chainlink_pricing
        union all
        select * from ssr_pricing
        union all
        select * from ousd_pricing
        union all
        select * from mid_stone_pricing
        union all
        select * from dejaaa
    )

select * from prices order by blockchain asc, symbol asc, dt desc