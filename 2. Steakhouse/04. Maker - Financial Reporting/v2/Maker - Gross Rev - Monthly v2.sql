/*
-- @title: Maker - Revenues Composition - Monthly v2
-- @author: Steakhouse Financial
-- @description: displays the monthly gross interest revenues for the last 4 years, allowing for a year-over-year comparison
-- @notes: Excludes the current month to avoid distortions, as the month is not yet complete and comparable
-- @version:
    - 1.0 - 2024-06-05 - Initial version
*/

select
    year,
    month,
    case month
        when 1 then 'Jan'
        when 2 then 'Feb'
        when 3 then 'Mar'
        when 4 then 'Apr'
        when 5 then 'May'
        when 6 then 'Jun'
        when 7 then 'Jul'
        when 8 then 'Aug'
        when 9 then 'Sep'
        when 10 then 'Oct'
        when 11 then 'Nov'
        when 12 then 'Dec'
        else 'Ein?'
    end as month_label,
    period,
    label_tab,
    value,
    value / 1e6 as value_m
from query_3735842 -- Maker - PNL
where fi_id = 1250 -- Stability Fee Revenues (Gross interest revenues)
and year > 2020
and not (extract(year from current_date) = year and extract(month from current_date) = month) -- exclude current month
order by year, month asc