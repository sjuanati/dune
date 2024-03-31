/*
-- @title: ENS - Holdings over time
-- @description: Calculates and tracks the USD holdings value (ENS, USDC, WETH, and ETH) for
    specific ENS-related wallets over time.
-- @author: Steakhouse Financial
-- @notes: 
    - Block 9380471 corresponds to the creation of ETH Registrar controller 3
    - Current ENS wallets are DAO wallet and ETH Registrar controllers 3 & 4
-- @version:
    - 2.0 - 2024-02-18 - Added comment header, added ETH Registrar 4, and added filter by block number
    - 1.0 - 2023-10-08 - Initial version
*/

with wallets as (
    select 0x283af0b28c62c092c9727f1ee09c02ca627eb7f5 as wallet, 'ETH Registrar 3' as name
    union all
    select 0x253553366Da8546fC250F225fe3d25d0C782303b as wallet, 'ETH Registrar 4' as name
    union all
    select 0xfe89cc7abb2c4183683ab71653c4cdc9b02d44b7 as wallet, 'DAO Wallet' as name
),
tokens as (
    select contract_address, symbol, decimals, power(10, decimals) as divisor
    from tokens.erc20
    where blockchain = 'ethereum'
        and contract_address in (
            0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48, -- USDC
            0xc18360217d8f7ab5e7c516566761ea12ce7f9d72, -- ENS
            0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 -- WETH
            )
),
tokenflows as (
    -- ERC-20 tokens: ENS, USDC and WETH
    select evt_block_time as ts, wallet, "to" as counterparty, contract_address, -cast(value as double) as qty
    from erc20_ethereum.evt_Transfer
    inner join tokens using (contract_address)
    inner join wallets on "from" = wallet
    where evt_block_number > 9380471 --2020.01.30
    union all
    select evt_block_time as ts, wallet, "from" as counterparty, contract_address, cast(value as double) as qty
    from erc20_ethereum.evt_Transfer
    inner join tokens using (contract_address)
    inner join wallets on "to" = wallet
    where evt_block_number > 9380471 --2020.01.30
    union all
    -- Non ERC-20 tokens: ETH
    select block_time as ts, wallet, "to" as counterparty, 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as contract_address, -cast(value as double) as qty
    from ethereum.traces
    inner join wallets on "from" = wallet
    where success = TRUE
        and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
        and to not in (0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2) -- WETH, doesn't have ERC20 mint
        and block_number > 9380471 --2020.01.30
    union all
    select block_time as ts, wallet, "from" as counterparty, 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as contract_address, cast(value as double) as qty
    from ethereum.traces
    inner join wallets on "to" = wallet
    where success = TRUE
        and (call_type not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
        and "from" not in (0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2) -- WETH, doesn't have ERC20 burn
        and block_number > 9380471 --2020.01.30
    union all
    -- Period sequence
    select period as ts, wallet, null as counterparty, contract_address, null as qty
    from unnest(sequence(date('2020-01-01'), current_date - interval '1' day, interval '1' day)) as t(period)
    cross join wallets
    cross join tokens
),
grp_asset_period as (
    select cast(ts as date) as period, wallet, contract_address, sum(qty) as qty
    from tokenflows
    group by 1, 2, 3
),
balance as (
    select period, wallet, contract_address, symbol, qty/divisor as delta, sum(qty) over (partition by wallet, contract_address order by period asc)/divisor as qty
    from grp_asset_period
    inner join tokens using (contract_address)
),
prices as (
    select date_trunc('day', minute) as period, contract_address, price
    from prices.usd
    inner join tokens using (contract_address)
    where blockchain = 'ethereum'
        and extract(hour from minute) = 23
        and extract(minute from minute) = 59
),
details_1 as (
    select period, wallet, contract_address, symbol, delta, qty, price as usd_price, qty*price as usd_value
    from balance
    inner join prices using (period, contract_address)
),
details_lag as (
    select period, wallet, contract_address, symbol, delta, qty, usd_price, usd_value, 
        lag(usd_price) over (partition by wallet, contract_address order by period asc) as lag_price,
        lag(usd_price) over (partition by wallet, contract_address order by period asc) as lag_qty
    from details_1
)
select period, symbol, sum(qty) as qty, usd_price, sum(usd_value) as usd_value
from details_lag
group by 1,2,4
order by 1 desc