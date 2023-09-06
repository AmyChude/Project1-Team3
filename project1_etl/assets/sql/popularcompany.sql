{% set config = {
    "extract_type": "full",
    "source_table_name": "jobs",
    "target_table_name": "popularcompany",
} %}

WITH CompanyMetrics AS (
    SELECT
        "employername",
        COUNT(DISTINCT jobid) AS NumberOfListings,
        SUM("applications") AS TotalApplications
    from 
    {{ config["source_table_name"] }}
    GROUP BY
        "employername"
)

SELECT
    cm."employername",
    cm.NumberOfListings,
    cm.TotalApplications
FROM
    CompanyMetrics cm
JOIN (
    SELECT
        MAX(NumberOfListings) AS MaxListings,
        MAX(TotalApplications) AS MaxApplications
    FROM
        CompanyMetrics
) AS max_values
ON
    cm.NumberOfListings = max_values.MaxListings
    OR cm.TotalApplications = max_values.MaxApplications;