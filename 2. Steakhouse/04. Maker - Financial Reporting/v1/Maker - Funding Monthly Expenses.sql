WITH
  pot_fees AS (
    SELECT
      call_block_time,
      rad / POWER(10, 45) AS fees
    FROM
      maker_ethereum.VAT_call_suck
    WHERE
      u = 0xa950524441892a31ebddf91d3ceefa04bf454466 /* Vow */
      AND v = 0x197e90f9fad81970ba7976f33cbd77088e5d7cf7 /* DSR */
      AND call_success
  )
SELECT
  DATE_FORMAT(call_block_time, '%Y-%m') AS period,
  SUM(fees) AS expenses
FROM
  pot_fees
GROUP BY
  1
ORDER BY
  1