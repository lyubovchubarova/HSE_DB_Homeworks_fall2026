drop database if exists hw4;
create database hw4;

create table hw4.vacancies_by_region (
    vacancy_snapshot_key String,
    snapshot_date Date,
    vacancy_id String,
    region_code LowCardinality(String),
    region_name LowCardinality(String),
    federal_district LowCardinality(String),
    sphere_name LowCardinality(String),
    creation_date Nullable(Date),
    modified_at Nullable(DateTime64(0, 'Europe/Moscow')),
    job_name String,
    salary_min Nullable(Decimal64(2)),
    salary_max Nullable(Decimal64(2)),
    currency Nullable(String),
    employment LowCardinality(String),
    schedule LowCardinality(String),
    education LowCardinality(String),
    experience_years Nullable(UInt8),
    work_places UInt32,
    is_quota Bool,
    is_special_workplace Bool
)
engine = MergeTree
order by (
    region_code,
    sphere_name,
    ifNull(creation_date, toDate('1970-01-01')),
    vacancy_id
);

create table hw4.vacancies_by_date as hw4.vacancies_by_region
engine = MergeTree
order by (
    ifNull(creation_date, toDate('1970-01-01')),
    federal_district,
    sphere_name,
    region_code,
    vacancy_id
);

create table hw4.vacancy_events as hw4.vacancies_by_region
engine = MergeTree
order by (snapshot_date, vacancy_id);

create table hw4.vacancy_mart (
    snapshot_date Date,
    region_code LowCardinality(String),
    region_name LowCardinality(String),
    sphere_name LowCardinality(String),
    currency String,
    vacancy_count UInt64,
    workplace_count UInt64,
    salary_min_sum Decimal(20, 2),
    salary_min_count UInt64
)
engine = SummingMergeTree(
    (vacancy_count, workplace_count, salary_min_sum, salary_min_count)
)
order by (snapshot_date, region_code, region_name, sphere_name, currency);

create materialized view hw4.vacancy_mart_mv to hw4.vacancy_mart as
select
    snapshot_date,
    region_code,
    region_name,
    sphere_name,
    ifNull(currency, 'Не указано') as currency,
    count() as vacancy_count,
    sum(work_places) as workplace_count,
    sum(ifNull(salary_min, toDecimal64(0, 2))) as salary_min_sum,
    countIf(isNotNull(salary_min)) as salary_min_count
from hw4.vacancy_events
group by snapshot_date, region_code, region_name, sphere_name, currency;
