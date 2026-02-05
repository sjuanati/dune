/*
-- @title: Core - Token info
-- @description: Provides the list of tokens for which the price will be retrieved
-- @author: Steakhouse Financial
-- @notes: N/A
-- @ER Model: https://docs.google.com/presentation/d/1JI2b12FBZaV0nInBFB2rbvQiXHBdylDHc1Kq3oJlA8M
-- @version:
    - 1.0 - 2025-02-06 - Initial version
    - 2.0 - 2025-03-04 - Add Pendle Tokens
    - 3.0 - 2025-03-15 - Incorporate price_address into final field
    - 4.0 - 2025-03-24 - Left join on prices table to fetch all tokens.erc20
                         Add regex to exclude tokens with symbols in them
    - 5.0 - 2025-03-26 - Include MetaMorpho Vault Symbols Tokens Info
    - 6.0 - 2025-03-27 - Code refactored, extending the list of tokens whether there is price feed or not
                       - Also filtering out scams/shitcoins based on symbol regex
    - 7.0 - 2025-04-02 - Add back in aero token in filter
    - 8.0 - 2025-04-09 - Add degen token in filter
    - 9.0 - 2025-05-02 - single_tokens cte by priority in case we are missing any field
                       - excluding tokens without at least one letter
                       - filter by chain also in cte <erc20_tokens>
                       - set type='stablecoin' for tokens in table erc20_stablecoins
   - 10.0 - 2025-05-09 - Add corn, ink & worldchain to chains
   - 11.0 - 2025-05-22 - Using prices.day instead of prices.usd_daily & dex.prices + removed fantom (replaced by sonic)
   - 12.0 - 2025-06-23 - Remove 'one' from where condition for exclusion
   - 13.0 - 2025-07-04 - Added katana chain
   - 14.0 - 2025-07-15 - Added hemi & tac chains
   - 15.0 - 2025-08-26 - Added unichain & plume chains
   - 16.0 - 2025-08-28 - Removing moar shitcoins in cte `clean_tokens`
                       - For chains Base & BNB, get tokens only from the 'token_info_inclusions'
   - 17.0 - 2025-09-09 - Added hyperevm chain
   - 18.0 - 2025-10-01 - Add start time as first chain block time
   - 19.0 - 2025-11-25 - Added monad & sei chains
   - 20.0 - 2025-12-08 - Add stable chain
   - 21.0 - 2025-12-22 - Split stablecoins by type (USD, EUR, ...)
*/

with
    -- blockchains to retrieve token prices from
    chains (blockchain) as (
        values
            'arbitrum',
            'avalanche_c',
            'base',
            'bnb',
            'celo',
            'corn',
            'ethereum',
            'gnosis',
            'hemi',
            'hyperevm',
            'ink',
            'katana',
            'linea',
            'monad',
            'optimism',
            'plume',
            'polygon',
            'plasma',
            'scroll',
            'sei',
            'sonic',
            'stable',
            'tac',
            'unichain',
            'worldchain',
            'zksync'
    ),
    -- tokens to retrieve prices from
    tokens as (
        -- manual inclusions dataset
        select
            blockchain,
            token_address as contract_address,
            symbol,
            decimals,
            true as has_price,
            "type",
            1 as priority,
            date (start_date) as start_date
        from dune.steakhouse.dataset_token_info_inclusions
        union all
        -- à-la-carte price calculations
        select
            blockchain,
            token_address as contract_address,
            symbol,
            null as decimals,
            true as price,
            null as "type",
            2 as priority,
            min(dt) as start_date
        from dune.steakhouse.result_token_alternative_price
        group by 1,2,3
        union all
        -- morpho vaults
        select
            chain as blockchain,
            metamorpho as contract_address,
            symbol,
            18 as decimals,
            false as has_price,
            'morpho' as "type",
            3 as priority,
            evt_block_time as start_date
        from (
            select * from metamorpho_factory_multichain.metamorphofactory_evt_createmetamorpho
            union all
            select * from metamorpho_factory_multichain.metamorphov1_1factory_evt_createmetamorpho
        )
        union all
        -- dune prices based on CoinPaprika or DEXes
        select
            blockchain,
            contract_address,
            symbol,
            decimals,
            true as has_price,
            null as "type",
            4 as priority,
            min(date("timestamp")) as start_date
        from prices.day
        join chains using (blockchain)
        where contract_address is not null
          and blockchain not in ('base', 'bnb') -- Only tokens defined in 'token_info_inclusions'
          and decimals >= 2
          and price < 2e6 -- avoid crazy prices too off (to be updated when BTC to the moon)
        group by 1,2,3,4
        having count(distinct("timestamp")) >= 12 -- at least 15 price updates to avoid scrap
    ),
    -- avoid duplicated tokens by choosing the values by priority, from 1 (highest) to 5 (lowest)
    single_tokens as (
        select
            blockchain as blockchain,
            contract_address as contract_address,
            min_by(symbol, priority) as symbol,
            min_by(decimals, priority) as decimals,
            min_by(has_price, priority) as has_price,
            min_by("type", priority) as "type",
            min_by(start_date, priority) as start_date
        from tokens
        group by 1,2
    ),
    -- full outer join with erc20 and erc20_stablecoins to retrieve any remaining missing values
    erc20_tokens as (
        select
            coalesce(t.blockchain, e.blockchain) as blockchain,
            coalesce(t.contract_address, e.contract_address) as contract_address,
            -- Temporary
            CASE 
                WHEN t.contract_address = 0x89d24a6b4ccb1b6faa2625fe562bdd9a23260359 THEN 'SAI'
                ELSE coalesce(e.symbol, t.symbol)
            END as symbol,
            coalesce(e.decimals, t.decimals) as decimals,
            if(t.contract_address is null, false, t.has_price) as has_price,
            case
                when sc.contract_address is not null
                 and coalesce(lower(t."type"), '') not like '%stablecoin%' -- avoid overriding a type set manually
                 and sc.symbol like '%USD%'
                then 'usd-stablecoin'
                when sc.contract_address is not null
                 and coalesce(lower(t."type"), '') not like '%stablecoin%' -- avoid overriding a type set manually
                 and sc.symbol like '%EUR%'
                then 'eur-stablecoin'
                when sc.contract_address is not null
                 and coalesce(lower(t."type"), '') not like '%stablecoin%' -- avoid overriding a type set manually
                then 'stablecoin'
                else coalesce(t."type", 'unknown')
            end as "type",
            t.start_date
        from single_tokens t
        full outer join tokens.erc20 e
            on t.blockchain = e.blockchain
            and t.contract_address = e.contract_address
        join chains c
            on coalesce(t.blockchain, e.blockchain) = c.blockchain
        left join tokens.erc20_stablecoins sc
          on sc.blockchain = coalesce(t.blockchain, e.blockchain)
         and sc.contract_address = coalesce(t.contract_address, e.contract_address)
    ),
    -- remove shitcoins based on symbol (scams, tests, rugs, memes...)
    clean_tokens as (
        select *
        from erc20_tokens
        where regexp_like(symbol, '^[A-Za-z0-9].*$') -- starts with alphanumeric (avoid _etc or $etc crap)
          and regexp_like(symbol, '[A-Za-z]') -- must have at least one letter, so exclude only numbers
          and not regexp_like(lower(symbol), '(moon|pump|100x|airdrop|claim|free|elon|trump|melania|doge|floki|maga|mega|meme|fomo|musk|meow|grok|rekt|wagmi|ngmi|rugpull|visit|dope|hodl|chill|hype|vitalik|voucher|buzz|scam)') -- scams 
          and not regexp_like(lower(symbol), '(test|bbb|ccc|ddd|eee|fff|ggg|hhh|iii|jjj|kkk|lll|mmm|nnn|ooo|ppp|qqq|rrr|sss|ttt|uuu|www|xxx|yyy|zzz|xyz|aab|aac|aad|aae|aaf|aag|aai|aaj|aak|aap)') -- tests
          and not regexp_like(lower(symbol), '(sex|masturbate|fuck|penis|kamasutra|penetrat|cumshot|threesome|gay|lesbian|fingering|asshole|orgasm|cock|bitch|bigballs)') -- sexual / bad wordz 
          and not regexp_like(lower(symbol), '(banana|pups|efm|pepe|maga|test|deepseek|machi|ansem|neiro|blast|byden|biden|goat|nvidia|kitty|virtual|popcat|robot|major|mania|mother|bunny|damn|tiktok|woman)') -- crap in Base 
          and not regexp_like(lower(symbol), '(brett|wolf|ronaldo|meme|moodeng|cult|daddy|kamala|pengu|byeden|bitcoin|kobe|peipei|barron|turbo|zuckerberg|eminem|andy|ivanka|simpson|pornhub|obama|harris|pork)') -- crap in Base 
          and not regexp_like(lower(symbol), '(wlfi|butt|mumu|retard|burger|billy|silly|pikachu|bobo|kekius|smile|fearnot|vance|taylor|hawktuah|skicat|bonk|conor|optimus|faggot|wojak|ebony|ponzi|girl|spank)') -- crap in Base 
          and not regexp_like(lower(symbol), '(brian|tremp|jonah|kanye|batman|goth|soprano|pokemon|peppa|mochi|neymar|kaito|hotwife|charlie|maneki|dream|terminus|castro|trunk|spiderman|brady|curry|slerf|coq)') -- crap in Base 
          and not regexp_like(lower(symbol), '(monkey|conan|dodo|coco|jogeco|pete|mario|shiro|tyson|cookie|broccoli|bogus|early|boden|higher|pirate|marvin|xirtam|onlyfud|shit|ponke|please|bebe|ceo|safu|peepo)') -- crap in Base
          and not regexp_like(lower(symbol), '(dogwifhat|dragonball|dividend_tracker|gangbang|frog|beast|beer|goku|michi|rabbit|shibtoshi|pizza|velocity|messi|bloomberg|btcbull|chicago|momo|harambe|dump|teddy)')
          and not regexp_like(lower(symbol), '(fart|french|pope|popo|jesus|google|amazon|spacex|party|captain|censored|ninja|tesla|hannibal|luigi|omikami|tuah|coca|honey|player|jeet|squid|useless|axoni|9inch)')
          and not regexp_like(lower(symbol), '(disney|pnut|birddog|sweet|sony|juice|dragonx)')
          and lower(symbol) not in (
              '0x0','1001lit','9inch','aaa','abc','act','aero','aerodrone','ai','ai16z','aibrain','aimc','akuma','allott','alpaca','alpha','anime','andy','ankai','anon','ape','apple','apu',
              'ass','asd','astrorider','aurora','away','bad','base','basepunks','based','basolana','bbc','benis','berry','bgme','bhb','bianca','bigbalz','binancedog','blackwomen','blade',
              'bleach','bmw','bob','boba','bone','booe','brexitx','bull','bully','buu','cake-lp','cat','chad','chaos','cheems','chiitan','clanker','codemaster',
              'coin','consent','cope','crc','cry','cum','d.o.g.e','dark','deadpool','deai','deep','deeplearn','deshare','dick','dividendtracker','dlp','dog','dogs','donald','donaldduck',
              'dontbuy','dope','down','draw','dsync','dyor-lp','elliot','ethereum','ferraform','fight','first','fist','fortress','tate',
              'fwog','fxckcat','gai','gates','gcr','genesissol','giga','ginnan','glaucus','gme','gold','goldrush','good morning','gpu','griffain','groyper',
              'gunner','hashai','hat','hello','hippo','home','hoppy','house','husky','iceberg','ilum','img','infinity','jlp','joe','joe rogan','jup','kabosu',
              'kai','kama','kamabla','kek','kendu','keycat','kim','king','kishu','labubu','laika','latina','legend','lightchain ai lcai','logadog','lol','love','lucky','luce','mad','mamo',
              'matt','meta','mew','miggles','mika','molly','mom','mog','money','morphai','mss','mtf','murad','mutant','mutuum finance','myceli','mystery','nai','naruto',
              'nature','nati','netflix','neural','new','nmt','nodeai','noice','notice','npc','nub','nubcat','nurse','nvidia','olympiad','opium_long','over','pac','panican','patriot','paw',
              'pepu','pika','pinu','pippin','pnut','pochita','poseidon','project89','psyop','putin','pyramid','rckt-v2','remittix rtx','retroart','retrocar','revenant',
              'rizzmas','rocket','rocky','rogsk','ronnie','santa','satoshi','sbf','scooby','sealdog','sec','share','shark','shib','shiba','shrub','sleepyjoe','slp','smoking','smr','smurfcat','snake',
              'snoopdogg','solidx','sonic','soon','spectre','spotmgtx','spurdo','spx','starlight','stonks','sunset','superjudge','swarms','swastika','taobot','talatchaudhry','tanuki',
              'tenderly','tetsuo','three','tiktok','timwalz','titcoin','titan','titanium','titanx','tkn','tnt','troll','trove','tst','ttt','ufo','under','uni-v2','upside','usa',
              'valium','vcegg','vertai','victorai','vine','vnlink','vvaifu','wagwag','web3','wen','white','why','wolverine','wooly','xmw','xxxx','yakuza','yolo','yes','zack','zebec network zbcn','zerebro',
              'zeus','zkcodex','zorb', 'milady' )
          and (length(trim(symbol)) > 2 or lower(symbol) in ('mt','wm', 'op')) -- too short
    ),
    final_tokens as (
        select
            c.blockchain,
            c.symbol,
            c.decimals,
            case 
                when e.first_block_time is not null and c.start_date < e.first_block_time then e.first_block_time
                when c.start_date is null and c.blockchain = 'katana' then TIMESTAMP'2025-05-08 10:20:12'
                when c.start_date is null and c.blockchain = 'tac' then TIMESTAMP'2025-06-11 10:49:04'
                else c.start_date
            end as start_date,
            c.contract_address as token_address,
            c."type",
            i.underlying_address,
            i.underlying_symbol,
            i.underlying_decimals,
            i.price_address,
            c.has_price
        from clean_tokens c
        left join dune.steakhouse.dataset_token_info_inclusions i
            on c.blockchain = i.blockchain
            and c.contract_address = i.token_address
        left join evms.info as e
            on c.blockchain = e.blockchain
    )

select * from final_tokens order by blockchain asc, symbol asc