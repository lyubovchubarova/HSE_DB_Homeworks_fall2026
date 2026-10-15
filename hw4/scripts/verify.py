#!/usr/bin/env python3

import hashlib
import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

if __package__:
    from .connections import ch_connect, pg_connect, wait_for_services
    from .data_contract import COLUMNS
    from .prepare import _write_json
    from .queries import PROFILES
else:
    from connections import ch_connect, pg_connect, wait_for_services
    from data_contract import COLUMNS
    from prepare import _write_json
    from queries import PROFILES


ROOT = Path(__file__).resolve().parents[1]
CLICKHOUSE_TABLES = ("vacancies_by_region", "vacancies_by_date")


def _canonical_value(value):
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc)
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        if value == 0:
            return "0"
        return format(value.normalize(), "f")
    return value


def canonical_rows(rows):
    normalized = [tuple(_canonical_value(value) for value in row) for row in rows]
    return sorted(normalized, key=lambda row: json.dumps(row, ensure_ascii=False, default=str))


def digest_rows(rows):
    payload = json.dumps(
        canonical_rows(rows),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _postgres_rows(connection):
    columns = ", ".join(COLUMNS)
    with connection.cursor() as cursor:
        cursor.execute(f"select {columns} from public.vacancies")
        return cursor.fetchall()


def _clickhouse_rows(client, table):
    columns = ", ".join(COLUMNS)
    return list(client.query(f"select {columns} from hw4.{table}").result_rows)


def _postgres_stats(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            select count(*), count(distinct (snapshot_date, vacancy_id)),
                   sum(work_places), min(creation_date), max(creation_date)
            from public.vacancies
            """
        )
        row = cursor.fetchone()
    return dict(zip(("rows", "unique_keys", "work_places", "min_date", "max_date"), row))


def _clickhouse_stats(client, table):
    row = client.query(
        f"""
        select count(), uniqExact(tuple(snapshot_date, vacancy_id)),
               sum(work_places), min(creation_date), max(creation_date)
        from hw4.{table}
        """
    ).first_row
    return dict(zip(("rows", "unique_keys", "work_places", "min_date", "max_date"), row))


def _postgres_query(connection, sql):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


def _clickhouse_query(client, sql):
    return list(client.query(sql).result_rows)


def verify_all():
    wait_for_services()
    pg_connection = pg_connect()
    ch_client = ch_connect()
    try:
        raw_rows = {"postgres": _postgres_rows(pg_connection)}
        raw_stats = {"postgres": _postgres_stats(pg_connection)}
        for table in CLICKHOUSE_TABLES:
            key = f"clickhouse_{table.removeprefix('vacancies_by_')}"
            raw_rows[key] = _clickhouse_rows(ch_client, table)
            raw_stats[key] = _clickhouse_stats(ch_client, table)

        raw_digests = {name: digest_rows(rows) for name, rows in raw_rows.items()}
        normalized_stats = {
            name: {key: _canonical_value(value) for key, value in stats.items()}
            for name, stats in raw_stats.items()
        }
        profiles = {}
        for profile in PROFILES:
            results = {"postgres": _postgres_query(pg_connection, profile.postgres)}
            for table in CLICKHOUSE_TABLES:
                key = f"clickhouse_{table.removeprefix('vacancies_by_')}"
                results[key] = _clickhouse_query(
                    ch_client,
                    profile.clickhouse.format(table=f"hw4.{table}"),
                )
            digests = {name: digest_rows(rows) for name, rows in results.items()}
            profiles[profile.name] = {
                "rows": {name: len(rows) for name, rows in results.items()},
                "digests": digests,
                "all_equal": len(set(digests.values())) == 1,
            }

        result = {
            "raw": {
                "stats": normalized_stats,
                "digests": raw_digests,
                "all_equal": (
                    len(set(raw_digests.values())) == 1
                    and len({json.dumps(value, sort_keys=True) for value in normalized_stats.values()}) == 1
                ),
            },
            "profiles": profiles,
        }
        result["all_equal"] = result["raw"]["all_equal"] and all(
            profile["all_equal"] for profile in profiles.values()
        )
        if not result["all_equal"]:
            raise ValueError(f"PostgreSQL and ClickHouse results differ: {result}")
        _write_json(ROOT / "evidence" / "integrity.json", result)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return result
    finally:
        pg_connection.close()
        ch_client.close()


if __name__ == "__main__":
    verify_all()

