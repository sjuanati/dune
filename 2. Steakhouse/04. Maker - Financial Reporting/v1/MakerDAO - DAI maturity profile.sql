WITH deltas AS 
(
    -- DAI (non DSR)
    SELECT dst AS wallet
    , date_trunc('day', "evt_block_time") AS dt
    , CAST(wad AS INT256) AS delta
    FROM maker_ethereum.DAI_evt_Transfer
    UNION ALL
    SELECT src AS wallet
    , date_trunc('day', "evt_block_time") AS dt
    , -CAST(wad AS INT256) AS delta
    FROM maker_ethereum.DAI_evt_Transfer
    
    -- Add DSR
    union all
    
    
        
    select 0x197E90f9FAD81970bA7976f33CbD77088E5D7cf7 as wallet, date_trunc('day', ts) as dt, dai_value * 1e18 as delta
    from dune.steakhouse.result_maker_accounting
    where cast(code as varchar) = '21110'
    
    -- Add Surplus Buffer
    union all
    select 0xa950524441892a31ebddf91d3ceefa04bf454466 as wallet, date_trunc('day', ts) as dt, dai_value * 1e18 as delta
    from dune.steakhouse.result_maker_accounting
    where cast(code as varchar) like '31%' and code <> 31810
    
), maturities AS 
(
    SELECT 'Speculative' AS wallet
    , maturity
    , weight
    FROM 
    (
        VALUES
        ('1-block', 0.122000000000000000000)
        , ('1-day', 0.122000000000000000000)
        , ('1-week', 0.234000000000000000000)
        , ('1-month', 0.061000000000000000000)
        , ('3-months', 0.09000000000000000000)
        , ('1-year', 0.371000000000000000000)
    ) AS t(maturity, weight)
    UNION ALL
    SELECT 'Organic' AS wallet
    , maturity
    , weight
    FROM 
    (
        VALUES
        ('1-block', 0.076000000000000000000)
        , ('1-day', 0.076000000000000000000)
        , ('1-week', 0.029000000000000000000)
        , ('1-month', 0.00000000000000000000)
        , ('3-months', 0.00000000000000000000)
        , ('1-year', 0.819000000000000000000)
    ) AS t(maturity, weight)    
    UNION ALL
    SELECT 'Surplus Buffer' AS wallet
    , maturity
    , weight
    FROM 
    (
        VALUES
        ('1-block', 0.0)
        , ('1-day', 0.0)
        , ('1-week', 0.0)
        , ('1-month', 0.0)
        , ('3-months', 0.00)
        , ('1-year', 1.0)
    ) AS t(maturity, weight)
), contracts /* Fix duplicates in the contract table */ AS 
(
    SELECT address, 1 AS id
    FROM ethereum.contracts
    GROUP BY 1
), grouped AS
(
    SELECT CASE WHEN wallet = 0xa950524441892a31ebddf91d3ceefa04bf454466 then 'Surplus Buffer'
        WHEN wallet = 0x197E90f9FAD81970bA7976f33CbD77088E5D7cf7 then 'Speculative'
        WHEN c.id IS NULL THEN 'Organic'
        ELSE 'Speculative' END AS wallet
    , dt
    , SUM(delta) AS delta
    FROM deltas
    LEFT JOIN contracts AS c
    ON wallet = address
    WHERE wallet <> 0x0000000000000000000000000000000000000000
    GROUP BY 1,2
), balances AS 
(
    SELECT wallet
    , dt
    , SUM(delta) OVER (PARTITION BY wallet ORDER BY dt)/1e18 AS balance
    FROM grouped
),
data as (
    SELECT maturity
    , dt
    , SUM(balance * weight) AS outflow
    , SUM(case when wallet <> 'Surplus Buffer' then balance * weight end) AS outflow_dai_only
    , SUM(case when wallet = 'Surplus Buffer' then balance * weight end) AS outflow_surplus_buffer
    FROM balances
    JOIN maturities USING (wallet)
    GROUP BY 1,2
),
data_2 as (
    select maturity, dt, sum(outflow) as outflow, sum(outflow_dai_only) as outflow_dai_only, sum(outflow_surplus_buffer) as outflow_surplus_buffer
    from data
    group by 1, 2
)
select *, sum(outflow) over (partition by dt) as total_period
from data_2
where dt < current_date
ORDER BY dt DESC NULLS FIRST