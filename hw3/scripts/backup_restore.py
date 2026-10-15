#!/usr/bin/env python3
"""Back up, restore and compare the vacancy database."""

import json
import os
import subprocess
from pathlib import Path

import psycopg2

from benchmark import query_sql, result_digest
from cluster import dsn, wait_for_roles


ROOT = Path(__file__).resolve().parents[1]
BACKUP = ROOT / "evidence" / "backup" / "hw3.dump"
CONTRACT_MARKER = "hw3-vacancy-mart-v1"
EXPECTED_VACANCY_COLUMNS = (
    "vacancy_snapshot_key",
    "snapshot_date",
    "vacancy_id",
    "region_code",
    "region_name",
    "federal_district",
    "sphere_name",
    "creation_date",
    "modified_at",
    "job_name",
    "salary_min",
    "salary_max",
    "currency",
    "employment",
    "schedule",
    "education",
    "experience_years",
    "work_places",
    "is_quota",
    "is_special_workplace",
)
if os.environ.get("PG_BIN"):
    PG_BIN = Path(os.environ["PG_BIN"])
else:
    PG_BIN = (
        Path(
            subprocess.check_output(
                ["brew", "--prefix", "postgresql@14"],
                text=True,
            ).strip()
        )
        / "bin"
    )
PG_DUMP = str(PG_BIN / "pg_dump")
PG_RESTORE = str(PG_BIN / "pg_restore")


def run(command):
    environment = os.environ.copy()
    environment["PGPASSWORD"] = "postgres"
    completed = subprocess.run(
        command,
        check=True,
        text=True,
        capture_output=True,
        env=environment,
    )
    return {
        "command": " ".join(command),
        "stdout": completed.stdout.strip(),
        "stderr": completed.stderr.strip(),
        "returncode": completed.returncode,
    }


def fetch_database_checks(node, database):
    checks = {}
    tables = (
        "control.vacancies",
        "benchmark.vacancies_base",
        "benchmark.vacancies_indexed",
        "benchmark.vacancies_partitioned",
    )
    with psycopg2.connect(dsn(node, database)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select table_schema, table_name
                from information_schema.tables
                where table_schema in ('control', 'benchmark', 'admin')
                  and table_type = 'BASE TABLE'
                order by table_schema, table_name
                """
            )
            checks["tables"] = cursor.fetchall()
            cursor.execute(
                """
                select n.nspname, t.relname, i.relname, pg_get_indexdef(i.oid)
                from pg_index x
                join pg_class i on i.oid = x.indexrelid
                join pg_class t on t.oid = x.indrelid
                join pg_namespace n on n.oid = t.relnamespace
                where n.nspname in ('control', 'benchmark', 'admin')
                order by n.nspname, t.relname, i.relname
                """
            )
            checks["indexes"] = cursor.fetchall()
            cursor.execute(
                """
                select n.nspname, c.relname, con.conname, con.contype,
                       pg_get_constraintdef(con.oid)
                from pg_constraint con
                join pg_class c on c.oid = con.conrelid
                join pg_namespace n on n.oid = c.relnamespace
                where n.nspname in ('control', 'benchmark', 'admin')
                order by n.nspname, c.relname, con.conname
                """
            )
            checks["constraints"] = cursor.fetchall()
            cursor.execute(
                """
                select n.nspname, c.relname, pg_get_expr(c.relpartbound, c.oid)
                from pg_class c
                join pg_namespace n on n.oid = c.relnamespace
                where n.nspname = 'benchmark' and c.relispartition
                order by c.relname
                """
            )
            checks["partition_bounds"] = cursor.fetchall()

            checks["vacancy_columns"] = {}
            checks["relations"] = {}
            for table in tables:
                schema_name, table_name = table.split(".", 1)
                cursor.execute(
                    """
                    select column_name
                    from information_schema.columns
                    where table_schema = %s and table_name = %s
                    order by ordinal_position
                    """,
                    (schema_name, table_name),
                )
                columns = tuple(row[0] for row in cursor.fetchall())
                if columns != EXPECTED_VACANCY_COLUMNS:
                    raise ValueError(f"{table} has incompatible columns: {columns}")
                checks["vacancy_columns"][table] = columns

                cursor.execute(
                    f"""
                    select
                        count(*),
                        count(vacancy_snapshot_key),
                        count(distinct vacancy_snapshot_key),
                        count(distinct (snapshot_date, vacancy_id)),
                        count(*) filter (
                            where vacancy_snapshot_key is null
                               or snapshot_date is null
                               or vacancy_id is null
                               or region_code is null
                               or sphere_name is null
                               or job_name is null
                               or work_places is null
                               or work_places <= 0
                               or (
                                   salary_min is not null
                                   and salary_max is not null
                                   and salary_min > salary_max
                               )
                        )
                    from {table}
                    """
                )
                row = cursor.fetchone()
                checks["relations"][table] = {
                    "rows": row[0],
                    "filled_keys": row[1],
                    "unique_keys": row[2],
                    "unique_business_keys": row[3],
                    "invalid_rows": row[4],
                }

            cursor.execute(
                "select * from control.vacancies order by vacancy_snapshot_key"
            )
            control_rows = cursor.fetchall()
            checks["control_sha256"] = result_digest(control_rows)
            checks["control_rows"] = len(control_rows)

            cursor.execute(query_sql("benchmark.vacancies_base"))
            report_rows = cursor.fetchall()
            checks["report"] = {
                "rows": len(report_rows),
                "sha256": result_digest(report_rows),
            }
    return checks


def main():
    leader, _ = wait_for_roles()
    BACKUP.parent.mkdir(parents=True, exist_ok=True)

    dump = run(
        [
            PG_DUMP,
            "-h", "127.0.0.1",
            "-p", str(leader["port"]),
            "-U", "postgres",
            "-d", "hw3",
            "-Fc",
            "--no-owner",
            "--no-privileges",
            "-f", str(BACKUP),
        ]
    )

    admin = psycopg2.connect(dsn(leader, "postgres"))
    try:
        admin.autocommit = True
        with admin.cursor() as cursor:
            cursor.execute(
                """
                select pg_terminate_backend(pid)
                from pg_stat_activity
                where datname = 'hw3_restore' and pid <> pg_backend_pid()
                """
            )
            cursor.execute("drop database if exists hw3_restore")
            cursor.execute("create database hw3_restore")
    finally:
        admin.close()

    restore = run(
        [
            PG_RESTORE,
            "-h", "127.0.0.1",
            "-p", str(leader["port"]),
            "-U", "postgres",
            "-d", "hw3_restore",
            "--exit-on-error",
            "--no-owner",
            "--no-privileges",
            str(BACKUP),
        ]
    )

    source = fetch_database_checks(leader, "hw3")
    restored = fetch_database_checks(leader, "hw3_restore")
    equal = source == restored
    evidence = {
        "evidence_contract": CONTRACT_MARKER,
        "leader": leader["name"],
        "pg_dump_version": run([PG_DUMP, "--version"])["stdout"],
        "pg_restore_version": run([PG_RESTORE, "--version"])["stdout"],
        "format": "PostgreSQL custom (-Fc)",
        "backup_file": str(BACKUP.relative_to(ROOT)),
        "backup_bytes": BACKUP.stat().st_size,
        "dump": dump,
        "restore": restore,
        "source": source,
        "restored": restored,
        "all_checks_equal": equal,
    }
    output = ROOT / "evidence" / "restore.json"
    output.write_text(
        json.dumps(evidence, ensure_ascii=False, default=str, indent=2) + "\n",
        encoding="utf-8",
    )
    if not equal:
        raise SystemExit("source and restored database checks differ")
    print(json.dumps(evidence, ensure_ascii=False, default=str, indent=2))


if __name__ == "__main__":
    main()

