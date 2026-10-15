select distinct
    md5(sphere_name) as sphere_key,
    sphere_name
from {{ ref('stg_trudvsem_vacancies') }}

