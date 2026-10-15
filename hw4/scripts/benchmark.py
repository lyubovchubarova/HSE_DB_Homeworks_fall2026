#!/usr/bin/env python3

import csv
import json
import statistics
import time
from pathlib import Path

if __package__:
    from .connections import ch_connect, pg_connect, wait_for_services
    from .prepare import _write_json
    from .queries import PROFILES
    from .verify import digest_rows
else:
    from connections import ch_connect, pg_connect, wait_for_services
    from prepare import _write_json
    from queries import PROFILES
    from verify import digest_rows


ROOT = Path(__file__).resolve().parents[1]
RUNS = 5
SYSTEMS = {
    "clickhouse_region": "hw4.vacancies_by_region",
    "clickhouse_date": "hw4.vacancies_by_date",
}


def summarize(values):
    values = [float(value) for value in values]
    return {
        "runs": len(values),
        "median_ms": round(statistics.median(values), 3),
        "min_ms": round(min(values), 3),
        "max_ms": round(max(values), 3),
    }


def _timed_postgres(connection, sql):
    started = time.perf_counter()
    with connection.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
    return (time.perf_counter() - started) * 1000, rows


def _timed_clickhouse(client, sql):
    started = time.perf_counter()
    result = client.query(sql)
    rows = list(result.result_rows)
    elapsed_ms = (time.perf_counter() - started) * 1000
    summary = result.summary or {}
    return elapsed_ms, rows, summary


def _save_plans(pg_connection, ch_client):
    plan_dir = ROOT / "evidence" / "plans"
    plan_dir.mkdir(parents=True, exist_ok=True)
    for profile in PROFILES:
        with pg_connection.cursor() as cursor:
            cursor.execute(f"explain (analyze, buffers, format json) {profile.postgres}")
            plan = cursor.fetchone()[0]
        _write_json(plan_dir / f"postgres_{profile.name}.json", plan)
        for system, table in SYSTEMS.items():
            sql = profile.clickhouse.format(table=table)
            rows = ch_client.query(f"explain indexes = 1 {sql}").result_rows
            (plan_dir / f"{system}_{profile.name}.txt").write_text(
                "\n".join(str(row[0]) for row in rows) + "\n",
                encoding="utf-8",
            )


def run_benchmark():
    wait_for_services()
    pg_connection = pg_connect()
    ch_client = ch_connect()
    records = []
    try:
        _save_plans(pg_connection, ch_client)
        for profile in PROFILES:
            _timed_postgres(pg_connection, profile.postgres)
            for run in range(1, RUNS + 1):
                elapsed, rows = _timed_postgres(pg_connection, profile.postgres)
                records.append({
                    "system": "postgres",
                    "profile": profile.name,
                    "run": run,
                    "elapsed_ms": round(elapsed, 3),
                    "result_digest": digest_rows(rows),
                    "read_rows": "",
                    "read_bytes": "",
                })

            for system, table in SYSTEMS.items():
                sql = profile.clickhouse.format(table=table)
                _timed_clickhouse(ch_client, sql)
                for run in range(1, RUNS + 1):
                    elapsed, rows, query_summary = _timed_clickhouse(ch_client, sql)
                    records.append({
                        "system": system,
                        "profile": profile.name,
                        "run": run,
                        "elapsed_ms": round(elapsed, 3),
                        "result_digest": digest_rows(rows),
                        "read_rows": query_summary.get("read_rows", ""),
                        "read_bytes": query_summary.get("read_bytes", ""),
                    })

        output = ROOT / "evidence" / "benchmark.csv"
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("w", encoding="utf-8", newline="") as target:
            writer = csv.DictWriter(target, fieldnames=tuple(records[0]))
            writer.writeheader()
            writer.writerows(records)

        summary = {}
        for profile in PROFILES:
            profile_records = [row for row in records if row["profile"] == profile.name]
            digests = {row["result_digest"] for row in profile_records}
            if len(digests) != 1:
                raise ValueError(f"result digests differ for {profile.name}")
            summary[profile.name] = {
                "result_digest": next(iter(digests)),
                "systems": {
                    system: summarize([
                        row["elapsed_ms"]
                        for row in profile_records
                        if row["system"] == system
                    ])
                    for system in ("postgres", *SYSTEMS)
                },
            }
        _write_json(ROOT / "evidence" / "benchmark_summary.json", summary)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return summary
    finally:
        pg_connection.close()
        ch_client.close()


if __name__ == "__main__":
    run_benchmark()
