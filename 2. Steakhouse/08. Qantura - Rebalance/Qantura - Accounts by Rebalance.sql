with
    -- coordinators that have a recipient; therefore, balancing stuff
    balance_coordinators as (
        select coordinator from gnosis_safe_gnosis.CoordinatorFactory_evt_NewCoordinator where recipient != 0x0000000000000000000000000000000000000000
        union
        select contract_address from gnosis_chain_gnosis.Coordinator_evt_NewRecipient
    ),
    -- all coordinators
    coordinators as (
        select
            nc.coordinator,
            if(bc.coordinator is not null, true, false) as is_rebalancer
        from gnosis_safe_gnosis.CoordinatorFactory_evt_NewCoordinator nc
        left join balance_coordinators bc on nc.coordinator = bc.coordinator
    ),
    users as (
        select
            account as addr,
            is_rebalancer
        from gnosis_safe_gnosis.AccountFactory_evt_NewAccount a
        inner join coordinators bc using(coordinator)
    ),
    totals as (
        select
            if(is_rebalancer = true, 'rebalance', 'non rebalance') as is_rebalancer,
            count(1) as num
        from users
        group by 1
    )

select * from totals
