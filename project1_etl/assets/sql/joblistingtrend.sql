{% set config = {
    "extract_type": "full",
    "source_table_name": "jobs"
} %}

SELECT
	TO_CHAR("date"::date, 'YYYY-MM') AS MonthYear,
    "jobtitle",
    COUNT(*) AS MonthlyListingCount,SUM("applications") AS MonthlyApplicationCount
from 
    {{ config["source_table_name"] }}
GROUP BY
    MonthYear,
    "jobtitle"
ORDER BY
    MonthYear,
    "jobtitle";