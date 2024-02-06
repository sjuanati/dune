WITH
    -- Calculate net GRO deposited or withdrawn in redemption by each user.
    gro_deposited AS (
        SELECT
            "date" AS "date",
            SUM("amount") AS "amount"
        FROM (
            SELECT
                date_trunc('day', "evt_block_time") AS "date",
                CAST("amount" AS DOUBLE) / 1e18 AS "amount"
            FROM gro_ethereum.RedemptionPool_evt_deposit
            UNION ALL
            SELECT
                date_trunc('day', "evt_block_time") AS "date",
                -CAST("amount" AS DOUBLE) / 1e18 AS "amount"
            FROM gro_ethereum.RedemptionPool_evt_withdraw
            )
        GROUP BY 1
    ),
    -- Calculate total GRO in redemption
    gro_deposited_total AS (
        SELECT SUM("amount") AS "amount"
        FROM gro_deposited
    ),
    -- Calculate total GRO claimed from redemption
    gro_claimed AS (
        SELECT SUM("amount") / 1e18 AS "amount"
        FROM gro_ethereum.RedemptionPool_evt_claim
    ),
    -- Calculate total cUSDC deposited in redemption
    cusdc_deposited AS (
        SELECT COALESCE(SUM(CAST("amount" AS DOUBLE) / 1e8), 0) AS "cusdc_deposited"
        FROM gro_ethereum.RedemptionPool_evt_CUSDCDeposit
        WHERE evt_block_number <= 18428468 -- redemption deadline block
    ),
    cusdc_price AS (
        SELECT 
            --"price" AS "cusdc_value"
            0.0231 AS "cusdc_value" -- at the time of redemption
        FROM prices.usd
        WHERE contract_address = 0x39AA39c021dfbaE8faC545936693aC917d5E7563
        ORDER BY "minute"
        DESC limit 1
    ),
    gro AS (
        SELECT
            CASE
                WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -CAST("value" AS DOUBLE)
                WHEN "from" = 0x0000000000000000000000000000000000000000 THEN CAST("value" AS DOUBLE)
            END / 1e18 AS amount
        FROM gro_ethereum.GROToken_evt_Transfer
        WHERE evt_block_number <= 18428468 -- redemption deadline block
    ),
    -- TODO: add vested amounts from main, team & investors
    gro_total AS (
        SELECT SUM(amount) AS "amount" FROM gro
    ),
    redemption_value AS (
        SELECT
            cd."cusdc_deposited" * cp."cusdc_value" AS "treasury_value",
            CASE
                WHEN cd."cusdc_deposited" = 0 THEN 0
                WHEN cp."cusdc_value" = 0 THEN 0
                ELSE  gt."amount" / (cd."cusdc_deposited" * cp."cusdc_value")
            END AS "theo_redemption_value",
            CASE
                WHEN cd."cusdc_deposited" = 0 THEN 0
                WHEN cp."cusdc_value" = 0 THEN 0
                ELSE (cd."cusdc_deposited" * cp."cusdc_value") / gdt."amount"
            END AS "redemption_value"
        FROM cusdc_deposited cd, cusdc_price cp, gro_deposited_total gdt, gro_total gt
    )

SELECT
    gd."date" AS "date",
    gd."amount" AS "gro_deposited",
    SUM(COALESCE(gd."amount", 0)) OVER (ORDER BY "date" ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) as "gro_deposited_acc",
    gdt."amount" * "redemption_value" AS "redemption_value",
    gc."amount" AS "usdc_claimed",
    cusdc."cusdc_deposited" AS "total_cusdc_deposited",
    cusdc_v."cusdc_value",
    rv."treasury_value",
    rv."redemption_value" AS "redemption_value2",
    0.25 AS "theo_redemption_value"--rv."theo_redemption_value"
FROM
    gro_deposited gd,
    gro_deposited_total gdt,
    gro_claimed gc,
    cusdc_deposited cusdc,
    cusdc_price cusdc_v,
    redemption_value rv
ORDER BY 1 DESC
