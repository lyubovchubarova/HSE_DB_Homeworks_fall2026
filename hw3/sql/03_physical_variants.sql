create table benchmark.vacancies_indexed
    (like benchmark.vacancies_base including defaults including constraints);

insert into benchmark.vacancies_indexed
select * from benchmark.vacancies_base;

create index vacancies_indexed_region_sphere_creation_idx
    on benchmark.vacancies_indexed (region_code, sphere_name, creation_date);

create table benchmark.vacancies_partitioned
    (like benchmark.vacancies_base including defaults including constraints)
    partition by range (snapshot_date);

create table benchmark.vacancies_partitioned_2026
    partition of benchmark.vacancies_partitioned
    for values from ('2026-01-01') to ('2027-01-01');

create table benchmark.vacancies_partitioned_default
    partition of benchmark.vacancies_partitioned default;

insert into benchmark.vacancies_partitioned
select * from benchmark.vacancies_base;

