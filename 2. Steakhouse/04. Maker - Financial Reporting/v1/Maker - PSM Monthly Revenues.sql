WITH
  psms AS (
    SELECT DISTINCT
      "u" AS psm_address
    FROM
      maker_ethereum.VAT_call_frob
    WHERE REPLACE(FROM_UTF8(i),U&'\0000', '') LIKE 'PSM-%'
    AND call_success
  ),
  psm_tx AS (
    SELECT
      call_tx_hash AS tx,
      call_block_time,
      REPLACE(FROM_UTF8(i),U&'\0000', '') AS ilk,
      SUM(dink / CAST(POWER(10, 18) AS DOUBLE)) AS amount
    FROM
      maker_ethereum.VAT_call_frob
    WHERE REPLACE(FROM_UTF8(i),U&'\0000', '') LIKE 'PSM-%'
    AND call_success
    GROUP BY
      1,
      2,
      3
  ),
  tx_metadata AS (
    SELECT
      tx,
      "from" AS tx_from,
      "to" AS tx_to
    FROM
      ethereum."transactions"
      INNER JOIN psm_tx ON tx = hash
  ),
  tx_fees AS (
    SELECT
      call_tx_hash AS tx,
      SUM(rad / CAST(POWER(10, 45) AS DOUBLE)) AS fees
    FROM
      maker_ethereum.VAT_call_move
      INNER JOIN psm_tx ON tx = call_tx_hash
      INNER JOIN psms ON src = psm_address
    WHERE
      dst = 0xa950524441892a31ebddf91d3ceefa04bf454466
    GROUP BY
      1
  )
SELECT
  DATE_FORMAT(call_block_time, '%Y-%m') AS period,
  SUM(fees) AS revenues
FROM
  psm_tx
  LEFT JOIN tx_fees USING (tx)
GROUP BY
  1
ORDER BY
  1