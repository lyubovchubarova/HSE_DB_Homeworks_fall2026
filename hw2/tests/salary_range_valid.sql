select vacancy_snapshot_key
from {{ ref('fct_vacancies') }}
where salary_min is not null
  and salary_max is not null
  and salary_min > salary_max
