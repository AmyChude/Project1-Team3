{% set config = {
    "extract_type": "full",
    "source_table_name": "jobs"
} %}

SELECT
    "locationname",
    "jobtitle",
    AVG(CASE WHEN "minimumsalary" >= 10000 THEN "minimumsalary" ELSE NULL END) AS AvgMinSalary,
    AVG(CASE WHEN "maximumsalary" >= 10000 THEN "maximumsalary" ELSE NULL END) AS AvgMaxSalary
FROM
     {{ config["source_table_name"] }}
WHERE
    "minimumsalary" >= 10000 OR "maximumsalary" >= 10000
GROUP BY
    "locationname",
    "jobtitle";