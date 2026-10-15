#!/usr/bin/env python3
"""Create and load the HW3 vacancy tables."""

import json
import os
from pathlib import Path

import psycopg2

from cluster import dsn, wait_for_roles


ROOT = Path(__file__).resolve().parents[1]
CONTROL_CSV = Path(
    os.getenv("HW3_CONTROL_CSV", ROOT / "data" / "control_vacancies.csv")
)
REAL_CSV = Path(
    os.getenv("HW3_REAL_CSV", ROOT / "data" / "trudvsem_vacancies.csv")
)
REAL_METADATA = Path(
    os.getenv("HW3_REAL_METADATA", ROOT / "data" / "trudvsem_metadata.json")
)
COPY_COLUMNS = """
    vacancy_snapshot_key,
    snapshot_date,
    vacancy_id,
    region_code,
    region_name,
    federal_district,
    sphere_name,
    creation_date,
    modified_at,
    job_name,
    salary_min,
    salary_max,
    currency,
    employment,
    schedule,
    education,
    experience_years,
    work_places,
    is_quota,
    is_special_workplace
"""


def execute_file(cursor, path):
    cursor.execute(path.read_text(encoding="utf-8"))


def main():
    leader, replica = wait_for_roles()
    connection = psycopg2.connect(dsn(leader, "postgres"))
    try:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select pg_terminate_backend(pid)
                from pg_stat_activity
                where datname in ('hw3', 'hw3_restore')
                  and pid <> pg_backend_pid()
                """
            )
            cursor.execute("drop database if exists hw3_restore")
            cursor.execute("drop database if exists hw3")
            cursor.execute("create database hw3")
    finally:
        connection.close()

    connection = psycopg2.connect(dsn(leader))
    try:
        connection.autocommit = True
        with connection.cursor() as cursor:
            execute_file(cursor, ROOT / "sql" / "01_schema.sql")
            with CONTROL_CSV.open(encoding="utf-8") as source:
                cursor.copy_expert(
                    f"""
                    copy control.vacancies ({COPY_COLUMNS})
                    from stdin with (format csv, header true)
                    """,
                    source,
                )
            execute_file(cursor, ROOT / "sql" / "02_benchmark_base.sql")
            with REAL_CSV.open(encoding="utf-8") as source:
                cursor.copy_expert(
                    f"""
                    copy benchmark.vacancies_base ({COPY_COLUMNS})
                    from stdin with (format csv, header true)
                    """,
                    source,
                )
            execute_file(cursor, ROOT / "sql" / "03_physical_variants.sql")
            for table in (
                "benchmark.vacancies_base",
                "benchmark.vacancies_indexed",
                "benchmark.vacancies_partitioned",
            ):
                cursor.execute(f"vacuum (analyze) {table}")

            cursor.execute(
                """
                select
                    (select count(*) from control.vacancies),
                    (select count(*) from benchmark.vacancies_base),
                    (select count(*) from benchmark.vacancies_indexed),
                    (select count(*) from benchmark.vacancies_partitioned),
                    (select min(snapshot_date) from benchmark.vacancies_base),
                    (select max(snapshot_date) from benchmark.vacancies_base),
                    (
                        select count(distinct vacancy_snapshot_key)
                        from benchmark.vacancies_base
                    ),
                    (
                        select count(distinct (snapshot_date, vacancy_id))
                        from benchmark.vacancies_base
                    )
                """
            )
            row = cursor.fetchone()
            if len({row[1], row[2], row[3], row[6], row[7]}) != 1:
                raise ValueError(f"vacancy cardinality mismatch: {row}")
            cursor.execute(
                """
                select relname, pg_total_relation_size(relid)
                from pg_catalog.pg_statio_user_tables
                where schemaname in ('control', 'benchmark')
                order by relname
                """
            )
            sizes = {name: size for name, size in cursor.fetchall()}
    finally:
        connection.close()

    evidence = {
        "leader": leader["name"],
        "replica": replica["name"],
        "control_rows": row[0],
        "base_rows": row[1],
        "indexed_rows": row[2],
        "partitioned_rows": row[3],
        "snapshot_min": str(row[4]),
        "snapshot_max": str(row[5]),
        "unique_keys": row[6],
        "unique_business_keys": row[7],
        "relation_bytes": sizes,
        "source_metadata": json.loads(REAL_METADATA.read_text(encoding="utf-8")),
    }
    output = ROOT / "evidence" / "setup.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(evidence, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(evidence, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

