SELECT 'FlapFlop' AS contract_type
    , data_binary AS contract_address
FROM maker_ethereum.vow_call_file
WHERE data_binary is not NULL
    AND call_success
GROUP BY data_binary

UNION ALL

SELECT 'PSM' AS contract_type
    , u AS contract_address
FROM maker_ethereum.vat_call_frob
WHERE from_utf8(bytearray_rtrim(i)) LIKE 'PSM%'
    AND call_success
GROUP BY u
