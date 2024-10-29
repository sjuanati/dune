/*
@dev:
    EURe v1 : 0xcb444e90d8198415266c6a2724b7900fb12fc56e
    EURe v2 : 0x420ca0f9b9b604ce0fd9c18ef134c705e5fa3430
    GBPe : 0x5cb9073902f2035222b9749f8fb0c9bfe5527108
    Coordinators are instances of CoordinatorFactory
*/

with
    periods as (
        select period from unnest(sequence(date('2024-09-02'), date_trunc('week', current_date), interval '7' day)) as t(period)
    ),
    -- coordinators that have a recipient; therefore, rebalancing stuff
    rebalance_coordinators as (
        select coordinator from gnosis_safe_gnosis.CoordinatorFactory_evt_NewCoordinator where recipient != 0x0000000000000000000000000000000000000000
        union
        select contract_address from gnosis_chain_gnosis.Coordinator_evt_NewRecipient
    ),
    -- accounts linked to balance coordinators
    rebalance_accounts as (
        select account
        from gnosis_safe_gnosis.AccountFactory_evt_NewAccount a
        inner join rebalance_coordinators bc using(coordinator)
    ),
    -- distinct wallets per period
    distinct_wallets as (
        select distinct
            date_trunc('week', t.block_date) as period,
            a.account as acc
        from cow_protocol_gnosis.trades t
        inner join (select distinct account from gnosis_safe_gnosis.AccountFactory_evt_NewAccount) a
        on t.trader = a.account
    ),
    -- cumulated accounts with any interaction with balancer coordinators via cowswap
    rebalance_accounts_cum as (
        select
            da1.period,
            count(distinct da2.acc) as cum_accounts
        from distinct_wallets da1
        join distinct_wallets da2 on da2.period <= da1.period
        group by 1
    ),
    -- @dev: Coordinators can be used for saving (recipient = 0x) or for rebalancing (recipient != 0x).
    -- The recipient can be assigned to a Coordinator in two ways:
    -- 1) When the coordinator is created: CoordinatorFactory->NewCoordinator where recipient != 0
    -- 2) After the coordinator is created: Coordinator->NewRecipient
    coordinators_start as (
        select
            date_trunc('week', evt_block_date) as period,
            count(
                if(recipient = 0x0000000000000000000000000000000000000000, 1, null)
            ) as saving_coord,
            count(
                if(recipient = 0x0000000000000000000000000000000000000000, null, 1)
            ) as rebalance_coord
        from gnosis_safe_gnosis.CoordinatorFactory_evt_NewCoordinator
        group by 1
    ),
    coordinators_after as (
        select
            date_trunc('week', evt_block_date) as period,
            count(1) as rebalance_coord_after
        from gnosis_chain_gnosis.Coordinator_evt_NewRecipient
        group by 1
    ),
    coordinators as (
        select
            period,
            saving_coord - coalesce(rebalance_coord_after, 0) as saving_coord,
            rebalance_coord + coalesce(rebalance_coord_after, 0) as rebalance_coord
        from coordinators_start
        full join coordinators_after using(period)
    ),
    -- trades in Cowswap where the trader is an account linked to a coordinator
    trades as (
        select
            date_trunc('week', t.block_date) as period,
            count(1) as tx_count,
            count(
                case
                    when 0xcb444e90d8198415266c6a2724b7900fb12fc56e in (buy_token_address, sell_token_address) then 1
                    else null
                end
            ) as tx_count_eur,
            count(
                case
                    when 0x5cb9073902f2035222b9749f8fb0c9bfe5527108 in (buy_token_address, sell_token_address) then 1
                    else null
                end
            ) as tx_count_gbp,
            cum_accounts,
            count(distinct a.account) as active_accounts,
            sum(
                case
                    when buy_token_address = 0xcb444e90d8198415266c6a2724b7900fb12fc56e then coalesce(units_bought, 0)
                    when sell_token_address = 0xcb444e90d8198415266c6a2724b7900fb12fc56e then coalesce(units_sold, 0)
                    else 0
                end
            ) as amount_eur,
            sum(
                case
                    when buy_token_address = 0x5cb9073902f2035222b9749f8fb0c9bfe5527108 then coalesce(units_bought, 0)
                    when sell_token_address = 0x5cb9073902f2035222b9749f8fb0c9bfe5527108 then coalesce(units_sold, 0)
                    else 0
                end
            ) as amount_gbp,
            sum(coalesce(t.usd_value, 0)) as total_amount_usd,
            sum(
                case
                    when 0xcb444e90d8198415266c6a2724b7900fb12fc56e in (buy_token_address, sell_token_address) then coalesce(t.usd_value, 0)
                    else 0
                end
            ) as amount_eur_usd,
            sum(
                case
                    when 0x5cb9073902f2035222b9749f8fb0c9bfe5527108 in (buy_token_address, sell_token_address) then coalesce(t.usd_value, 0)
                    else 0
                end
            ) as amount_gbp_usd
        from cow_protocol_gnosis.trades t
        inner join (select distinct account from gnosis_safe_gnosis.AccountFactory_evt_NewAccount) a
            on t.trader = a.account
        left join rebalance_accounts_cum bac on date_trunc('week', t.block_date) = bac.period
        where date(t.block_date) > date('2024-09-01')
        group by 1, 5
    ),
    totals as (
        select
            period,
            coalesce(c.saving_coord, 0) as saving_coord,
            coalesce(c.rebalance_coord, 0) as rebalance_coord,
            coalesce(c.saving_coord, 0) + coalesce(c.rebalance_coord, 0) as total_coord,
            t.active_accounts as active_accounts,
            t.cum_accounts as cum_active_accounts,
            if(t.cum_accounts != 0, cast(t.active_accounts as double) / cast(t.cum_accounts as double), 0) as active_accounts_per,
            coalesce(t.tx_count, 0) as tx,
            coalesce(t.tx_count_eur, 0) as tx_eur,
            coalesce(t.tx_count_gbp, 0) as tx_gbp,
            coalesce(t.amount_eur, 0) as amount_eur,
            coalesce(t.amount_gbp, 0) as amount_gbp,
            coalesce(t.total_amount_usd, 0) as total_amount_usd,
            t.amount_eur_usd,
            t.amount_gbp_usd
        from periods
        left join coordinators c using (period)
        left join trades t using (period)
    ),
    totals_cum as (
        select
            *,
            sum(tx) over (order by period asc) as cum_tx,
            sum(amount_eur) over (order by period asc) as cum_amount_eur,
            sum(amount_gbp) over (order by period asc) as cum_amount_gbp,
            sum(total_amount_usd) over (order by period asc) as cum_total_amount_usd,
            sum(saving_coord) over (order by period asc) as cum_saving_coord,
            sum(rebalance_coord) over (order by period asc) as cum_rebalance_coord,
            sum(saving_coord + rebalance_coord) over (order by period asc) as cum_coord
        from totals
    ),
    totals_cum_coord as (
        select
            *,
            coalesce(if(cum_coord != 0, cast(cum_rebalance_coord as double) / cast(cum_coord as double)), 0) as cum_rebalance_coord_per
        from totals_cum
    ),
    -- get gnosis pay users & volumes to benchmark them vs. qantura
    totals_pay as (
        select
            period,
            t.*,
            gp.users as pay_users,
            gp.value_eur as pay_amount_eur,
            gp.value_gbp as pay_amount_gbp
        from totals_cum_coord t
        left join query_4143340 gp using (period)
    )

select * from totals_pay order by period desc
