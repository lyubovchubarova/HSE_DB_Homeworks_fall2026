#!/usr/bin/env python3
"""Run a few readable data-quality checks and save their results."""

import json
import os
from pathlib import Path

import psycopg2


DSN = os.getenv(
    "VACANCY_DWH_DSN",
    "host=127.0.0.1 port=55432 dbname=vacancy_dwh user=postgres",
)
OUTPUT = Path("evidence/diagnostics.json")

CHECKS = {
    "duplicate_snapshot_keys": """
        select count(*) - count(distinct (snapshot_date, vacancy_id))
        from raw.trudvsem_vacancies
    """,
    "invalid_salary_ranges": """
        select count(*)
        from analytics_candidate.fct_vacancies
        where salary_min is not null
          and salary_max is not null
          and salary_min > salary_max
    """,
    "creation_after_snapshot": """
        select count(*)
        from analytics_candidate.fct_vacancies
        where creation_date > snapshot_date
    """,
    "published_rows": """
        select count(*)
        from published.mart_regional_sphere_demand
    """,
}


def run_checks(connection):
    results = {}
    with connection.cursor() as cursor:
        for name, query in CHECKS.items():
            cursor.execute(query)
            results[name] = cursor.fetchone()[0]
    results["passed"] = (
        results["duplicate_snapshot_keys"] == 0
        and results["invalid_salary_ranges"] == 0
        and results["creation_after_snapshot"] == 0
        and results["published_rows"] > 0
    )
    return results


def main():
    with psycopg2.connect(DSN) as connection:
        results = run_checks(connection)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(
        json.dumps(results, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(results, ensure_ascii=False))
    if not results["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
