select
    md5(concat_ws(':', snapshot_date::text, vacancy_id)) as vacancy_snapshot_key,
    snapshot_date,
    to_char(snapshot_date, 'YYYYMMDD')::integer as snapshot_date_key,
    vacancy_id,
    md5(region_code) as region_key,
    md5(sphere_name) as sphere_key,
    creation_date,
    modified_at,
    job_name,
    salary_min,
    salary_max,
    currency,
    employment,
    schedule,
    education,
    experience_years,
    work_places,
    is_quota,
    is_special_workplace
from {{ ref('stg_trudvsem_vacancies') }}

