WITH mkr_prices AS
(
    SELECT minute, price
    FROM prices.usd
    WHERE contract_address = 0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2
    AND blockchain = 'ethereum'
)
SELECT * FROM mkr_prices
WHERE 
(
    (EXTRACT(MINUTE FROM minute) = 59 AND EXTRACT(HOUR FROM minute) = 23)
    OR
    minute = (SELECT MAX(minute) FROM mkr_prices)
)
AND DATE(minute) >= DATE('2020-01-01')
ORDER BY minute DESC