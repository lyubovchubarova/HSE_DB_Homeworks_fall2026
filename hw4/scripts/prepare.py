#!/usr/bin/env python3

import json
import os
import tempfile
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

if __package__:
    from .connections import ch_connect, pg_connect, wait_for_services
    from .data_contract import COLUMNS, read_csv, validate_batches
    from .mart import aggregate_rows, direct_rows, mart_rows, rows_digest
else:
    from connections import ch_connect, pg_connect, wait_for_services
    from data_contract import COLUMNS, read_csv, validate_batches
    from mart import aggregate_rows, direct_rows, mart_rows, rows_digest


ROOT = Path(__file__).resolve().parents[1]
INITIAL_CSV = ROOT / "data" / "vacancies_initial.csv"
NEW_CSV = ROOT / "data" / "vacancies_new_batch.csv"


def _optional(value, parser):
    return None if value in (None, "") else parser(value)


def _boolean(value):
    normalized = str(value).strip().lower()
    if normalized not in {"true", "false"}:
        raise ValueError(f"invalid boolean: {value!r}")
    return normalized == "true"


def to_clickhouse_row(row):
    values = dict(row)
    values["snapshot_date"] = date.fromisoformat(row["snapshot_date"])
    values["creation_date"] = _optional(row.get("creation_date"), date.fromisoformat)
    values["modified_at"] = _optional(row.get("modified_at"), datetime.fromisoformat)
    values["salary_min"] = _optional(row.get("salary_min"), Decimal)
    values["salary_max"] = _optional(row.get("salary_max"), Decimal)
    values["currency"] = None if row.get("currency") in (None, "") else row["currency"]
    values["experience_years"] = _optional(row.get("experience_years"), int)
    values["work_places"] = int(row["work_places"])
    values["is_quota"] = _boolean(row["is_quota"])
    values["is_special_workplace"] = _boolean(row["is_special_workplace"])
    return tuple(values[column] for column in COLUMNS)


def _execute_clickhouse_ddl(client, path):
    statements = [
        statement.strip()
        for statement in path.read_text(encoding="utf-8").split(";")
        if statement.strip()
    ]
    for statement in statements:
        client.command(statement)


def _copy_postgres(cursor, path):
    columns = ", ".join(COLUMNS)
    with Path(path).open(encoding="utf-8") as source:
        cursor.copy_expert(
            f"copy public.vacancies ({columns}) from stdin with (format csv, header true)",
            source,
        )


def _analyze_postgres(cursor):
    cursor.execute("analyze public.vacancies")


def _insert_clickhouse(client, table, rows):
    client.insert(
        f"hw4.{table}",
        [to_clickhouse_row(row) for row in rows],
        column_names=list(COLUMNS),
    )


def _counts(pg_connection, ch_client):
    with pg_connection.cursor() as cursor:
        cursor.execute("select count(*) from public.vacancies")
        pg_count = cursor.fetchone()[0]
    ch_counts = {
        table: ch_client.command(f"select count() from hw4.{table}")
        for table in ("vacancies_by_region", "vacancies_by_date", "vacancy_events")
    }
    return {"postgres": pg_count, **ch_counts}


def _json_default(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    raise TypeError(f"cannot serialize {type(value).__name__}")


def _write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".part",
            delete=False,
        ) as target:
            temporary_path = Path(target.name)
            json.dump(value, target, ensure_ascii=False, indent=2, default=_json_default)
            target.write("\n")
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary_path, path)
    except Exception:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise


def validate_split_batch_probe(partial_rows, row):
    result = {
        "vacancy_count": int(row[0]),
        "workplace_count": int(row[1]),
        "salary_min_sum": format(row[2], ".2f"),
        "salary_min_count": int(row[3]),
        "average_salary_min": format(row[4], ".2f"),
    }
    expected = {
        "vacancy_count": 3,
        "workplace_count": 4,
        "salary_min_sum": "600.00",
        "salary_min_count": 2,
        "average_salary_min": "300.00",
    }
    evidence = {
        "insert_blocks": 2,
        "partial_rows": int(partial_rows),
        "contains_null_salary": True,
        "result": result,
        "expected": expected,
        "matches_expected": int(partial_rows) >= 2 and result == expected,
    }
    if not evidence["matches_expected"]:
        raise ValueError(f"split-batch mart probe failed: {evidence}")
    return evidence


def _run_split_batch_probe(client):
    for statement in (
        "drop view if exists hw4.vacancy_mart_probe_mv",
        "drop table if exists hw4.vacancy_mart_probe_events",
        "drop table if exists hw4.vacancy_mart_probe",
        """
        create table hw4.vacancy_mart_probe_events (
            probe_key String,
            salary_min Nullable(Decimal64(2)),
            work_places UInt32
        ) engine = MergeTree order by probe_key
        """,
        """
        create table hw4.vacancy_mart_probe (
            probe_key String,
            vacancy_count UInt64,
            workplace_count UInt64,
            salary_min_sum Decimal(20, 2),
            salary_min_count UInt64
        ) engine = SummingMergeTree(
            (vacancy_count, workplace_count, salary_min_sum, salary_min_count)
        ) order by probe_key
        """,
        """
        create materialized view hw4.vacancy_mart_probe_mv
        to hw4.vacancy_mart_probe as
        select
            probe_key,
            count() as vacancy_count,
            sum(work_places) as workplace_count,
            sum(ifNull(salary_min, toDecimal64(0, 2))) as salary_min_sum,
            countIf(isNotNull(salary_min)) as salary_min_count
        from hw4.vacancy_mart_probe_events
        group by probe_key
        """,
    ):
        client.command(statement)
    try:
        client.command("system stop merges hw4.vacancy_mart_probe")
        client.insert(
            "hw4.vacancy_mart_probe_events",
            [("same-key", Decimal("100.00"), 1), ("same-key", None, 1)],
            column_names=["probe_key", "salary_min", "work_places"],
        )
        client.insert(
            "hw4.vacancy_mart_probe_events",
            [("same-key", Decimal("500.00"), 2)],
            column_names=["probe_key", "salary_min", "work_places"],
        )
        partial_rows = client.command("select count() from hw4.vacancy_mart_probe")
        row = client.query(
            """
            select
                sum(vacancy_count),
                sum(workplace_count),
                sum(salary_min_sum),
                sum(salary_min_count),
                round(
                    toDecimal128(sum(salary_min_sum), 6) /
                        sum(salary_min_count),
                    2
                )
            from hw4.vacancy_mart_probe
            """
        ).first_row
        return validate_split_batch_probe(partial_rows, row)
    finally:
        client.command("system start merges hw4.vacancy_mart_probe")
        client.command("drop view if exists hw4.vacancy_mart_probe_mv")
        client.command("drop table if exists hw4.vacancy_mart_probe_events")
        client.command("drop table if exists hw4.vacancy_mart_probe")


def load_all(initial_path=INITIAL_CSV, new_path=NEW_CSV):
    initial_rows = read_csv(initial_path)
    new_rows = read_csv(new_path)
    contract = validate_batches(initial_rows, new_rows)
    wait_for_services()

    pg_connection = pg_connect()
    ch_client = ch_connect(database="default")
    try:
        with pg_connection.cursor() as cursor:
            cursor.execute((ROOT / "sql" / "postgres.sql").read_text(encoding="utf-8"))
            _copy_postgres(cursor, initial_path)
        pg_connection.commit()

        _execute_clickhouse_ddl(ch_client, ROOT / "sql" / "clickhouse.sql")
        for table in ("vacancies_by_region", "vacancies_by_date", "vacancy_events"):
            _insert_clickhouse(ch_client, table, initial_rows)

        initial_counts = _counts(pg_connection, ch_client)
        initial_mart = mart_rows(ch_client)
        initial_direct = direct_rows(ch_client)
        if initial_mart != initial_direct:
            raise ValueError("incremental mart differs from direct initial aggregation")

        with pg_connection.cursor() as cursor:
            _copy_postgres(cursor, new_path)
        pg_connection.commit()
        with pg_connection.cursor() as cursor:
            _analyze_postgres(cursor)
        pg_connection.commit()
        for table in ("vacancies_by_region", "vacancies_by_date", "vacancy_events"):
            _insert_clickhouse(ch_client, table, new_rows)

        final_counts = _counts(pg_connection, ch_client)
        final_mart = mart_rows(ch_client)
        final_direct = direct_rows(ch_client)
        if final_mart != final_direct:
            raise ValueError("incremental mart differs from direct final aggregation")
        split_batch_probe = _run_split_batch_probe(ch_client)

        expected_initial = contract["initial_rows"]
        expected_final = contract["total_rows"]
        if set(initial_counts.values()) != {expected_initial}:
            raise ValueError(f"initial row counts differ: {initial_counts}")
        if set(final_counts.values()) != {expected_final}:
            raise ValueError(f"final row counts differ: {final_counts}")

        load_evidence = {
            "contract": contract,
            "initial_counts": initial_counts,
            "final_counts": final_counts,
            "new_rows_added": expected_final - expected_initial,
        }
        mart_evidence = {
            "initial_groups": len(initial_mart),
            "final_groups": len(final_mart),
            "initial_digest": rows_digest(initial_mart),
            "initial_direct_digest": rows_digest(initial_direct),
            "final_digest": rows_digest(final_mart),
            "final_direct_digest": rows_digest(final_direct),
            "initial_matches_direct": initial_mart == initial_direct,
            "final_matches_direct": final_mart == final_direct,
            "initial_totals": aggregate_rows(initial_rows),
            "final_totals": aggregate_rows(initial_rows + new_rows),
            "split_batch_probe": split_batch_probe,
        }
        _write_json(ROOT / "evidence" / "load.json", load_evidence)
        _write_json(ROOT / "evidence" / "mart_increment.json", mart_evidence)
        result = {"load": load_evidence, "mart": mart_evidence}
        print(json.dumps(result, ensure_ascii=False, indent=2, default=_json_default))
        return result
    finally:
        pg_connection.close()
        ch_client.close()


def main():
    load_all()


if __name__ == "__main__":
    main()
