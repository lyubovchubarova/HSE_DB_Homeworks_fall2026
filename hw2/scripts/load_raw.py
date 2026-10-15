#!/usr/bin/env python3
"""Load a sanitized Работа России snapshot into PostgreSQL."""

import argparse
import gzip
import json
import os
from datetime import date
from pathlib import Path

import psycopg2
from psycopg2.extras import Json, execute_values


DEFAULT_DSN = "host=127.0.0.1 port=55432 dbname=vacancy_dwh user=postgres"


def read_snapshot(path):
    rows = []
    with gzip.open(path, "rt", encoding="utf-8") as source:
        for line_number, line in enumerate(source, start=1):
            if not line.strip():
                continue
            row = json.loads(line)
            if not isinstance(row, dict):
                raise ValueError(f"line {line_number} is not a JSON object")
            rows.append(row)
    return rows


def _validate_rows(rows):
    if not rows:
        raise ValueError("snapshot is empty")

    snapshot_dates = {row.get("snapshot_date") for row in rows}
    if None in snapshot_dates or len(snapshot_dates) != 1:
        raise ValueError("all rows must have one snapshot_date")

    seen = set()
    for row in rows:
        vacancy_id = row.get("vacancy_id")
        if not vacancy_id:
            raise ValueError("each row must have vacancy_id")
        key = (row["snapshot_date"], str(vacancy_id))
        if key in seen:
            raise ValueError(f"duplicate snapshot key: {key}")
        seen.add(key)

    snapshot_date = date.fromisoformat(next(iter(snapshot_dates)))
    return snapshot_date


def load_vacancies(connection, rows, source_file):
    snapshot_date = _validate_rows(rows)
    values = [
        (
            snapshot_date,
            str(row["vacancy_id"]),
            Json(row),
            str(source_file),
        )
        for row in rows
    ]
    try:
        with connection.cursor() as cur:
            cur.execute("create schema if not exists raw")
            cur.execute(
                """
                create table if not exists raw.trudvsem_vacancies (
                    snapshot_date date not null,
                    vacancy_id text not null,
                    payload jsonb not null,
                    source_file text not null,
                    loaded_at timestamptz not null default now(),
                    primary key (snapshot_date, vacancy_id)
                )
                """
            )
            cur.execute(
                "delete from raw.trudvsem_vacancies where snapshot_date = %s",
                (snapshot_date,),
            )
            execute_values(
                cur,
                """
                insert into raw.trudvsem_vacancies (
                    snapshot_date, vacancy_id, payload, source_file
                ) values %s
                """,
                values,
                page_size=1000,
            )
        connection.commit()
        return len(values)
    except Exception:
        connection.rollback()
        raise


def main():
    parser = argparse.ArgumentParser(description="Load Работа России JSONL.GZ")
    parser.add_argument("path", type=Path)
    parser.add_argument("--dsn", default=os.getenv("VACANCY_DWH_DSN", DEFAULT_DSN))
    args = parser.parse_args()

    rows = read_snapshot(args.path)
    with psycopg2.connect(args.dsn) as connection:
        loaded = load_vacancies(connection, rows, args.path.name)
    print(f"snapshot={rows[0]['snapshot_date']} loaded={loaded}")


if __name__ == "__main__":
    main()
