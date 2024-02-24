/*
-- @title: Steakhouse - ENS - Materialized View Updates
-- @description: Checks whether all MVs used in the ENS dashboard have been refreshed after the
                 scheduled daily loads between 01:00 AM and 03:00 AM UTC.
-- @author: Steakhouse Financial
-- @notes:
    - Filtering largest tables by date to improve performance (only the last days are needed)
    - TODO: Some MVs might be scheduled at different times, so 24h to 27h window needs to be confirmed
-- @version:
    - 1.1 - 2024-02-19 - Added hyperlinks to all materialized views
    - 1.0 - 2024-02-15 - Initial release
*/

WITH
    latest_periods AS (
        SELECT
            'result_ens_accounting_main <a href="https://dune.com/queries/2244104" target="_blank"> 🔗 </a>' AS name,
            MAX(ts) AS period
        FROM dune.steakhouse.result_ens_accounting_main
        WHERE ts > date '2024-01-01'
        UNION ALL
        SELECT
            'result_ens_accounting_assets_m2m <a href="https://dune.com/queries/1848781" target="_blank"> 🔗 </a>' AS name,
            MAX(period) AS period
        FROM dune.steakhouse.result_ens_accounting_assets_m2m
        UNION ALL
        SELECT
            'result_ens_accounting_expenses <a href="https://dune.com/queries/1862442" target="_blank"> 🔗 </a>' AS name,
            MAX(period) AS period
        FROM dune.steakhouse.result_ens_accounting_expenses
        UNION ALL
        SELECT
            'result_ens_accounting_revenues <a href="https://dune.com/queries/1849106" target="_blank"> 🔗 </a>' AS name,
            MAX(period) AS period
        FROM dune.steakhouse.result_ens_accounting_revenues
        WHERE period > date '2024-01-01'
        UNION ALL
        SELECT
            'result_ens_accounting_swaps <a href="https://dune.com/queries/2237617" target="_blank"> 🔗 </a>' AS name,
            MAX(ts) AS period
        FROM dune.steakhouse.result_ens_accounting_swaps
        UNION ALL
        SELECT
            'result_ens_accounting_transfers <a href="https://dune.com/queries/1954014" target="_blank"> 🔗 </a>' AS name,
            MAX(ts) AS period
        FROM dune.steakhouse.result_ens_accounting_transfers
        UNION ALL
        SELECT
            'result_ens_endaoment_aura_reth_strategy <a href="https://dune.com/queries/2940992" target="_blank"> 🔗 </a>' AS name,
            MAX(ts) AS period
        FROM dune.steakhouse.result_ens_endaoment_aura_reth_strategy
        UNION ALL
        SELECT
            'result_ens_endaoment_ankreth_strategy <a href="https://dune.com/queries/3233411" target="_blank"> 🔗 </a>' AS name,
            MAX(period) AS period
        FROM dune.steakhouse.result_ens_endaoment_ankreth_strategy
        UNION ALL
        SELECT
            'result_ens_endaoment_aura_wsteth_strategy <a href="https://dune.com/queries/2077810" target="_blank"> 🔗 </a>' AS name,
            MAX(ts) AS period
        FROM dune.steakhouse.result_ens_endaoment_aura_wsteth_strategy
        UNION ALL
        SELECT
            'result_ens_endaoment_comp_strategies <a href="https://dune.com/queries/2010788" target="_blank"> 🔗 </a>' AS name,
            MAX(ts) AS period
        FROM dune.steakhouse.result_ens_endaoment_comp_strategies
        UNION ALL
        SELECT
            'result_ens_endaoment_comp_v3_usdc_strategy <a href="https://dune.com/queries/3067922" target="_blank"> 🔗 </a>' AS name,
            MAX(ts) AS period
        FROM dune.steakhouse.result_ens_endaoment_comp_v3_usdc_strategy
        UNION ALL
        SELECT
            'result_ens_endaoment_comp_v3_weth_strategy <a href="https://dune.com/queries/3243523" target="_blank"> 🔗 </a>' AS name,
            MAX(ts) AS period
        FROM dune.steakhouse.result_ens_endaoment_comp_v3_weth_strategy
        UNION ALL
        SELECT
            'result_ens_endaoment_curve_steth_strategy <a href="https://dune.com/queries/2175403" target="_blank"> 🔗 </a>' AS name,
            MAX(ts) AS period
        FROM dune.steakhouse.result_ens_endaoment_curve_steth_strategy
        WHERE ts > date '2024-01-01'
        UNION ALL
        SELECT
            'result_ens_endaoment_dsr_strategy <a href="https://dune.com/queries/2894898" target="_blank"> 🔗 </a>' AS name,
            MAX(ts) AS period
        FROM dune.steakhouse.result_ens_endaoment_dsr_strategy
        WHERE ts > date '2024-01-01'
        UNION ALL
        SELECT
            'result_ens_endaoment_ethx_strategy <a href="https://dune.com/queries/3233323" target="_blank"> 🔗 </a>' AS name,
            MAX(period) AS period
        FROM dune.steakhouse.result_ens_endaoment_ethx_strategy
        UNION ALL
        SELECT
            'result_ens_endaoment_reth_strategy <a href="https://dune.com/queries/2965419" target="_blank"> 🔗 </a>' AS name,
            MAX(period) AS period
        FROM dune.steakhouse.result_ens_endaoment_reth_strategy
        UNION ALL
        SELECT
            'result_ens_endaoment_steth_strategy <a href="https://dune.com/queries/2281966" target="_blank"> 🔗 </a>' AS name,
            MAX(period) AS period
        FROM dune.steakhouse.result_ens_endaoment_steth_strategy
        UNION ALL
        SELECT
            'result_ens_endaoment_univ3_seth2_weth_strategy <a href="https://dune.com/queries/2175484" target="_blank"> 🔗 </a>' AS name,
            MAX(ts) AS period
        FROM dune.steakhouse.result_ens_endaoment_univ3_seth2_weth_strategy
        UNION ALL
        SELECT
            'result_ens_aethweth_strategy <a href="https://dune.com/queries/3255061" target="_blank"> 🔗 </a>' AS name,
            MAX(period) AS period
        FROM dune.steakhouse.result_ens_aethweth_strategy
        UNION ALL
        SELECT
            'result_ens_spweth_strategy <a href="https://dune.com/queries/3254892" target="_blank"> 🔗 </a>' AS name,
            MAX(period) AS period
        FROM dune.steakhouse.result_ens_spweth_strategy
    )

SELECT
    name,
    period,
    CASE
        WHEN CURRENT_DATE - period > interval '27' hour THEN '🔴'  -- data older than 27 hours
        WHEN CURRENT_DATE - period > interval '24' hour THEN '🟠'  -- data within 24-27 hours
        ELSE '🟢'  -- data within the last 24 hours
    END AS Status
FROM latest_periods
ORDER BY period ASC;
