select period, alm_sub_category,  alm_category, balance
from query_3293442 -- Maker - ALM - Liabilities detail per day
where period >= current_date - interval '180' day
order by period desc, balance desc
