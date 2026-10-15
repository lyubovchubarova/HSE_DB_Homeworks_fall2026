-- Один и тот же отчёт выполняется на base, indexed и partitioned.
select
    sphere_name,
    count(*)::bigint as vacancy_count,
    sum(work_places)::bigint as work_place_count,
    round(avg(salary_min), 2) as average_salary_min,
    round(avg(salary_max), 2) as average_salary_max
from benchmark.vacancies_base
where region_code = '0200000000000'
  and snapshot_date >= date '2026-01-01'
  and snapshot_date < date '2027-01-01'
  and creation_date >= date '2026-01-01'
group by sphere_name
order by vacancy_count desc, sphere_name;

