drop schema if exists control cascade;
drop schema if exists benchmark cascade;

create schema control;
create schema benchmark;

create table control.vacancies (
    vacancy_snapshot_key text primary key,
    snapshot_date date not null,
    vacancy_id text not null,
    region_code text not null,
    region_name text not null,
    federal_district text not null,
    sphere_name text not null,
    creation_date date,
    modified_at timestamptz,
    job_name text not null,
    salary_min numeric,
    salary_max numeric,
    currency text,
    employment text not null,
    schedule text not null,
    education text not null,
    experience_years integer,
    work_places integer not null check (work_places > 0),
    is_quota boolean not null,
    is_special_workplace boolean not null,
    unique (snapshot_date, vacancy_id),
    check (salary_min is null or salary_max is null or salary_min <= salary_max)
);

