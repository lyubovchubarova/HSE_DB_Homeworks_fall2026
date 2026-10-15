select vacancy_snapshot_key
from {{ ref('fct_vacancies') }}
where creation_date > snapshot_date
