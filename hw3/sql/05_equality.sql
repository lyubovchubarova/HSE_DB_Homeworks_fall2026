with indexed_difference as (
    (select * from benchmark.vacancies_base
     except all
     select * from benchmark.vacancies_indexed)
    union all
    (select * from benchmark.vacancies_indexed
     except all
     select * from benchmark.vacancies_base)
),
partitioned_difference as (
    (select * from benchmark.vacancies_base
     except all
     select * from benchmark.vacancies_partitioned)
    union all
    (select * from benchmark.vacancies_partitioned
     except all
     select * from benchmark.vacancies_base)
)
select
    (select count(*) from indexed_difference) as indexed_differences,
    (select count(*) from partitioned_difference) as partitioned_differences;

select
    count(*) as rows,
    count(distinct vacancy_snapshot_key) as unique_keys,
    count(distinct (snapshot_date, vacancy_id)) as unique_business_keys,
    min(snapshot_date) as snapshot_min,
    max(snapshot_date) as snapshot_max
from control.vacancies;

