{% set config = {
    "extract_type": "full",
    "source_table_name": "jobs",
    "target_table_name": "jobsbylocation",
} %}

select Locationname, count(jobId) as NumberofListings
from 
    {{ config["source_table_name"] }}
group by locationname
order by 2 desc;