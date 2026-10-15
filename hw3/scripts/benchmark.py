#!/usr/bin/env python3
"""Benchmark one vacancy report on three physical table designs."""

import argparse
import csv
import hashlib
import json
import os
import statistics
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TABLES = {
    "base": "benchmark.vacancies_base",
    "indexed": "benchmark.vacancies_indexed",
    "partitioned": "benchmark.vacancies_partitioned",
}
RESULT_HEADERS = (
    "sphere_name",
    "vacancy_count",
    "work_place_count",
    "average_salary_min",
    "average_salary_max",
)


def result_digest(rows):
    encoded = json.dumps(
        rows,
        ensure_ascii=False,
        default=str,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def plan_metrics(plan):
    root = plan["Plan"]
    return {
        "planning_ms": float(plan["Planning Time"]),
        "execution_ms": float(plan["Execution Time"]),
        "result_rows": int(root["Actual Rows"]),
        "shared_hit_blocks": int(root.get("Shared Hit Blocks", 0)),
        "shared_read_blocks": int(root.get("Shared Read Blocks", 0)),
        "temp_read_blocks": int(root.get("Temp Read Blocks", 0)),
        "temp_written_blocks": int(root.get("Temp Written Blocks", 0)),
    }


def query_sql(table_name):
    if table_name not in TABLES.values():
        raise ValueError(f"unexpected table: {table_name}")
    return f"""
        select
            sphere_name,
            count(*)::bigint as vacancy_count,
            sum(work_places)::bigint as work_place_count,
            round(avg(salary_min), 2) as average_salary_min,
            round(avg(salary_max), 2) as average_salary_max
        from {table_name}
        where region_code = '0200000000000'
          and snapshot_date >= date '2026-01-01'
          and snapshot_date < date '2027-01-01'
          and creation_date >= date '2026-01-01'
        group by sphere_name
        order by vacancy_count desc, sphere_name
    """.strip()


def explain_json(cursor, sql):
    cursor.execute(f"explain (analyze, buffers, format json) {sql}")
    return cursor.fetchone()[0][0]


def explain_text(cursor, sql):
    cursor.execute(f"explain (analyze, buffers, format text) {sql}")
    return "\n".join(row[0] for row in cursor.fetchall()) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Benchmark vacancy tables")
    parser.add_argument("--dsn", default=os.getenv("HW3_DSN"))
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "evidence" / "optimization",
    )
    args = parser.parse_args()
    if args.repeats < 5:
        parser.error("--repeats must be at least 5")

    import psycopg2
    if args.dsn is None:
        from cluster import dsn, wait_for_roles

        leader, _ = wait_for_roles()
        args.dsn = dsn(leader)

    args.output.mkdir(parents=True, exist_ok=True)
    plans_dir = args.output / "plans"
    plans_dir.mkdir(exist_ok=True)
    summary = {
        "measurement": {
            "warmups": 1,
            "measured_repeats": args.repeats,
            "time_source": "PostgreSQL EXPLAIN Execution Time",
        },
        "runs": [],
        "equality": {},
    }
    results = {}

    with psycopg2.connect(args.dsn) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            for variant, table in TABLES.items():
                cursor.execute(f"analyze {table}")
                sql = query_sql(table)
                explain_json(cursor, sql)
                measurements = [
                    plan_metrics(explain_json(cursor, sql))
                    for _ in range(args.repeats)
                ]
                cursor.execute(sql)
                rows = cursor.fetchall()
                results[variant] = rows
                times = [item["execution_ms"] for item in measurements]
                (plans_dir / f"{variant}.txt").write_text(
                    explain_text(cursor, sql),
                    encoding="utf-8",
                )
                summary["runs"].append(
                    {
                        "variant": variant,
                        "returned_rows": len(rows),
                        "result_sha256": result_digest(rows),
                        "execution_times_ms": times,
                        "median_execution_ms": statistics.median(times),
                        "measurements": measurements,
                    }
                )

            with (args.output / "result.csv").open(
                "w", encoding="utf-8", newline=""
            ) as target:
                writer = csv.writer(target)
                writer.writerow(RESULT_HEADERS)
                writer.writerows(results["base"])

            for variant in ("indexed", "partitioned"):
                summary["equality"][variant] = {
                    "equal": results[variant] == results["base"],
                    "base_sha256": result_digest(results["base"]),
                    "variant_sha256": result_digest(results[variant]),
                }

    output = args.output / "summary.json"
    output.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    if not all(item["equal"] for item in summary["equality"].values()):
        raise SystemExit("benchmark results differ")
    print(f"benchmark evidence written to {output}")


if __name__ == "__main__":
    main()

