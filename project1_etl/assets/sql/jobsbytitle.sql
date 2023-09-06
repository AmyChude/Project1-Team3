{% set config = {
    "extract_type": "full",
    "source_table_name": "jobs",
    "target_table_name": "jobsbytitle",
} %}

select jobTitle,
count(jobId) as NumberofListings
from 
    {{ config["source_table_name"] }}
group by jobtitle
order by 2 desc;
