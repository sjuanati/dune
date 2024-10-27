WITH days AS ( 
    SELECT day
    FROM unnest(sequence(
        timestamp'2024-08-01 00:00', 
        CAST(NOW() as timestamp),
        interval '1' day)
    ) as s(day)
)
, farm_addresses(address, vault_name) as (
    VALUES
    (0x0650CAF159C5A49f711e8169D4336ECB9b950275, 'Sky Farm'),
    (0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD, 'sUSDS Farm Vault'), -- DegenBox Abracabra
    (0x10ab606B067C9C461d8893c47C7512472E19e2Ce, 'Chronicle Farm')
)
, sky_flow as (
    SELECT 
        date_trunc('day', evt_block_time) as time, farm_addresses.address, vault_name
            , SUM(amount) OVER (PARTITION BY farm_addresses.address, vault_name ORDER BY date_trunc('day', evt_block_time)) as total_held
            , LEAD(date_trunc('day', evt_block_time), 1, now()) OVER (PARTITION BY farm_addresses.address, vault_name ORDER BY date_trunc('day', evt_block_time)) as next_day
    FROM (
        SELECT 
            evt_block_time 
            , "to" as address
            , (value * 1e-18) as amount
        FROM sky_ethereum.USDS_evt_Transfer as transfers
        UNION ALL
        SELECT evt_block_time 
            ,  "from" as address
            , -(value * 1e-18) as amount
        FROM sky_ethereum.USDS_evt_Transfer as transfers
    ) as eth_txs
    JOIN farm_addresses on eth_txs.address = farm_addresses.address
)
, balances_daily as (
    SELECT day, vault_name, case when total_held <= 1e-6 then 0 else total_held end as total_held
    FROM (
        SELECT days.day, vault_name, SUM(total_held) as total_held
        FROM sky_flow
        JOIN days on sky_flow.time <= days.day
        and days.day < sky_flow.next_day
        GROUP BY 1, 2
    )
)

SELECT day, vault_name, total_held
FROM balances_daily
