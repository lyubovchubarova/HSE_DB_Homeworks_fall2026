select
    fact.snapshot_date,
    region.region_code,
    region.region_name,
    region.federal_district,
    sphere.sphere_name,
    count(*)::bigint as vacancy_count,
    sum(fact.work_places)::bigint as work_place_count,
    round(
        avg(fact.salary_min) filter (
            where lower(coalesce(fact.currency, '')) like '%руб%'
        ),
        2
    ) as average_salary_min,
    round(
        avg(fact.salary_max) filter (
            where lower(coalesce(fact.currency, '')) like '%руб%'
        ),
        2
    ) as average_salary_max,
    round(
        avg(case when fact.experience_years = 0 then 1.0 else 0.0 end),
        4
    ) as no_experience_share
from {{ ref('fct_vacancies') }} fact
join {{ ref('dim_region') }} region using (region_key)
join {{ ref('dim_sphere') }} sphere using (sphere_key)
group by
    fact.snapshot_date,
    region.region_code,
    region.region_name,
    region.federal_district,
    sphere.sphere_name

