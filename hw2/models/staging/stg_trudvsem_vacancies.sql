with extracted as (
    select
        snapshot_date,
        vacancy_id,
        coalesce(nullif(trim(payload ->> 'source'), ''), 'Работа России') as source,
        coalesce(nullif(trim(payload ->> 'region_code'), ''), 'unknown') as region_code,
        coalesce(nullif(trim(payload ->> 'region_name'), ''), 'Не указано') as region_name,
        coalesce(
            nullif(trim(payload ->> 'federal_district'), ''),
            'Не указано'
        ) as federal_district,
        nullif(payload ->> 'creation_date', '')::date as creation_date,
        nullif(payload ->> 'modified_at', '')::timestamptz as modified_at,
        nullif(payload ->> 'salary_min', '')::numeric as source_salary_min,
        nullif(payload ->> 'salary_max', '')::numeric as source_salary_max,
        nullif(trim(payload ->> 'currency'), '') as currency,
        coalesce(nullif(trim(payload ->> 'job_name'), ''), 'Не указано') as job_name,
        coalesce(nullif(trim(payload ->> 'sphere_name'), ''), 'Не указано') as sphere_name,
        coalesce(nullif(trim(payload ->> 'employment'), ''), 'Не указано') as employment,
        coalesce(nullif(trim(payload ->> 'schedule'), ''), 'Не указано') as schedule,
        coalesce(nullif(trim(payload ->> 'education'), ''), 'Не указано') as education,
        {{ first_integer("payload ->> 'experience_years'") }} as experience_years,
        greatest(
            coalesce(nullif(payload ->> 'work_places', '')::integer, 1),
            1
        ) as work_places,
        coalesce((payload ->> 'is_quota')::boolean, false) as is_quota,
        coalesce((payload ->> 'is_special_workplace')::boolean, false)
            as is_special_workplace,
        loaded_at
    from {{ source('raw', 'trudvsem_vacancies') }}
)
select
    snapshot_date,
    vacancy_id,
    source,
    region_code,
    region_name,
    federal_district,
    creation_date,
    modified_at,
    case
        when source_salary_min is not null and source_salary_max is not null
            then least(source_salary_min, source_salary_max)
        else source_salary_min
    end as salary_min,
    case
        when source_salary_min is not null and source_salary_max is not null
            then greatest(source_salary_min, source_salary_max)
        else source_salary_max
    end as salary_max,
    currency,
    job_name,
    sphere_name,
    employment,
    schedule,
    education,
    experience_years,
    work_places,
    is_quota,
    is_special_workplace,
    loaded_at
from extracted
