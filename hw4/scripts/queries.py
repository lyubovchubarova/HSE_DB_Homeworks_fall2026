#!/usr/bin/env python3

from dataclasses import dataclass


@dataclass(frozen=True)
class QueryProfile:
    name: str
    postgres: str
    clickhouse: str


SELECTIVE_POSTGRES = """
select
    creation_date,
    count(*) as vacancy_count,
    sum(work_places) as workplace_count,
    round(sum(salary_min) / nullif(count(salary_min), 0), 2) as average_salary_min
from public.vacancies
where region_code = '3400000000000'
  and sphere_name = 'Здравоохранение и социальное обеспечение'
  and creation_date between date '2026-09-01' and date '2026-10-06'
group by creation_date
order by creation_date
"""

SELECTIVE_CLICKHOUSE = """
select
    creation_date,
    count() as vacancy_count,
    sum(work_places) as workplace_count,
    round(
        toDecimal128(sum(ifNull(salary_min, toDecimal64(0, 2))), 6) /
            nullIf(countIf(isNotNull(salary_min)), 0),
        2
    ) as average_salary_min
from {table}
where region_code = '3400000000000'
  and sphere_name = 'Здравоохранение и социальное обеспечение'
  and ifNull(creation_date, toDate('1970-01-01'))
      between toDate('2026-09-01') and toDate('2026-10-06')
group by creation_date
order by creation_date
"""

WIDE_POSTGRES = """
select
    federal_district,
    sphere_name,
    count(*) as vacancy_count,
    sum(work_places) as workplace_count,
    round(sum(salary_min) / nullif(count(salary_min), 0), 2) as average_salary_min
from public.vacancies
where creation_date >= date '2026-09-01'
group by federal_district, sphere_name
order by federal_district, sphere_name
"""

WIDE_CLICKHOUSE = """
select
    federal_district,
    sphere_name,
    count() as vacancy_count,
    sum(work_places) as workplace_count,
    round(
        toDecimal128(sum(ifNull(salary_min, toDecimal64(0, 2))), 6) /
            nullIf(countIf(isNotNull(salary_min)), 0),
        2
    ) as average_salary_min
from {table}
where ifNull(creation_date, toDate('1970-01-01')) >= toDate('2026-09-01')
group by federal_district, sphere_name
order by federal_district, sphere_name
"""

PROFILES = (
    QueryProfile("selective", SELECTIVE_POSTGRES, SELECTIVE_CLICKHOUSE),
    QueryProfile("wide", WIDE_POSTGRES, WIDE_CLICKHOUSE),
)
