select distinct on (region_code)
    md5(region_code) as region_key,
    region_code,
    region_name,
    federal_district
from {{ ref('stg_trudvsem_vacancies') }}
order by region_code, loaded_at desc

