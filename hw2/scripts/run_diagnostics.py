#!/usr/bin/env python3
import copy
import json
import os
import subprocess
import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.load_raw import load_events
from scripts.publish import publish_candidate


DSN = os.getenv(
    "KUDAGO_DWH_DSN",
    "host=127.0.0.1 port=55432 dbname=kudago_dwh user=postgres",
)
DBT = ROOT / ".venv/bin/dbt"
BASE = ROOT / "data/source/kudago_spb_summer_2026.json"


def payload(path):
    return json.loads(path.read_text(encoding="utf-8"))


def dbt(*args, check=True):
    result = subprocess.run(
        [str(DBT), *args, "--project-dir", str(ROOT), "--profiles-dir", str(ROOT)],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if check and result.returncode:
        raise RuntimeError(result.stdout[-4000:])
    return result


def rebuild():
    dbt("seed", "--full-refresh")
    dbt("run", "--full-refresh")


def load(connection, data, label, source_system="kudago", replace=False):
    return load_events(
        connection,
        data,
        label,
        source_system=source_system,
        replace_source=replace,
    )


def restore_baseline(connection):
    load(connection, payload(BASE), BASE.name, replace=True)
    with connection.cursor() as cur:
        cur.execute("delete from raw.kudago_events where source_system <> 'kudago'")
    connection.commit()


def scalar(connection, query, params=()):
    with connection.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchone()[0]


def fingerprint(connection, relation="analytics_public.mart_daily_events"):
    query = f"""
        select md5(coalesce(string_agg(row_to_json(t)::text, '|' order by mart_key), ''))
        from (select * from {relation} order by mart_key) t
    """
    return scalar(connection, query)


def main():
    results = {}
    baseline = payload(BASE)
    first_event = baseline["results"][0]
    first_event_id = first_event["id"]
    late_event, late_period = next(
        (event, period)
        for event in baseline["results"]
        for period in event["dates"]
        if isinstance(period.get("end"), int) and period["end"] > period["start"]
    )
    late_event_id = late_event["id"]
    late_start = late_period["start"]

    with psycopg2.connect(DSN) as connection:
        restore_baseline(connection)
        rebuild()
        dbt("test")
        publish_candidate(connection)
        baseline_fact_count = scalar(
            connection, "select count(*) from analytics_candidate.fct_event_occurrence"
        )

        duplicate = copy.deepcopy(baseline)
        duplicate["results"].append(copy.deepcopy(first_event))
        load(connection, duplicate, "duplicate_event", replace=True)
        rebuild()
        duplicate_facts = scalar(
            connection, "select count(*) from analytics_candidate.fct_event_occurrence"
        )
        results["duplicate"] = {
            "prediction": "duplicate does not multiply facts",
            "observed": duplicate_facts,
            "passed": duplicate_facts == baseline_fact_count,
        }

        restore_baseline(connection)
        rebuild()
        before = scalar(
            connection,
            """
            select duration_minutes
            from analytics_candidate.fct_event_occurrence
            where source_system = 'kudago'
              and event_id = %s
              and extract(epoch from start_at)::bigint = %s
            """,
            (late_event_id, late_start),
        )
        late_correction = copy.deepcopy(baseline)
        corrected_event = next(
            event for event in late_correction["results"] if event["id"] == late_event_id
        )
        corrected_period = next(
            period for period in corrected_event["dates"] if period["start"] == late_start
        )
        corrected_period["end"] += 3600
        load(connection, late_correction, "late_correction", replace=True)
        rebuild()
        after = scalar(
            connection,
            """
            select duration_minutes
            from analytics_candidate.fct_event_occurrence
            where source_system = 'kudago'
              and event_id = %s
              and extract(epoch from start_at)::bigint = %s
            """,
            (late_event_id, late_start),
        )
        results["late_correction"] = {
            "prediction": "same business key is updated, not duplicated",
            "before_minutes": float(before),
            "after_minutes": float(after),
            "passed": after == before + 60,
        }

        restore_baseline(connection)
        second_source = {"results": [copy.deepcopy(first_event)]}
        load(
            connection,
            second_source,
            "second_source",
            source_system="partner_test",
        )
        rebuild()
        source_count = scalar(
            connection,
            "select count(distinct source_system) from analytics_candidate.fct_event_occurrence where event_id=%s",
            (first_event_id,),
        )
        results["second_source"] = {
            "prediction": "equal local IDs from two sources stay separate",
            "observed_sources": source_count,
            "passed": source_count == 2,
        }

        seed = ROOT / "seeds/diagnostic_venue_history.csv"
        original_seed = seed.read_bytes()
        try:
            overlap_row = (
                "synthetic_history,100,Пересекающаяся версия,"
                "2026-05-01 00:00:00,2026-07-01 00:00:00\n"
            )
            seed.write_bytes(original_seed + overlap_row.encode("utf-8"))
            dbt("seed", "--full-refresh")
            overlap = dbt("test", "--select", "no_overlapping_venue_history", check=False)
        finally:
            seed.write_bytes(original_seed)
            dbt("seed", "--full-refresh")
        overlap_is_expected_failure = (
            overlap.returncode != 0
            and "no_overlapping_venue_history" in overlap.stdout
            and "Database Error" not in overlap.stdout
            and "Compilation Error" not in overlap.stdout
        )
        results["history_overlap"] = {
            "prediction": "overlap test fails",
            "dbt_exit_code": overlap.returncode,
            "passed": overlap_is_expected_failure,
        }

        restore_baseline(connection)
        rebuild()
        dbt("test")
        publish_candidate(connection)
        first = fingerprint(connection)
        restore_baseline(connection)
        rebuild()
        dbt("test")
        publish_candidate(connection)
        second = fingerprint(connection)
        results["repeat_run"] = {
            "first_hash": first,
            "second_hash": second,
            "passed": first == second,
        }

        stable_public = fingerprint(connection)
        bad_quality = copy.deepcopy(baseline)
        bad_quality["results"][0]["title"] = None
        load(connection, bad_quality, "bad_quality", replace=True)
        rebuild()
        bad_test = dbt("test", check=False)
        after_failure = fingerprint(connection)
        results["failed_quality_keeps_public"] = {
            "dbt_exit_code": bad_test.returncode,
            "before_hash": stable_public,
            "after_hash": after_failure,
            "passed": bad_test.returncode != 0 and stable_public == after_failure,
        }

        restore_baseline(connection)
        rebuild()
        dbt("test")

    results["all_passed"] = all(item["passed"] for item in results.values())
    output = ROOT / "evidence/diagnostics.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(results, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(results, ensure_ascii=False, indent=2))
    if not results["all_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
