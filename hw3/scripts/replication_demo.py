#!/usr/bin/env python3
import json
import time
from pathlib import Path

import psycopg2
import requests

from cluster import dsn, wait_for_roles


ROOT = Path(__file__).resolve().parents[1]
BEFORE_MARKER = "leader-to-replica-before-switchover"
AFTER_MARKER = "leader-to-replica-after-switchover"
REJECTED_MARKER = "must-not-be-written-on-replica"


def sql_role(node):
    with psycopg2.connect(dsn(node)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select
                    pg_is_in_recovery(),
                    current_setting('transaction_read_only'),
                    current_database(),
                    version()
                """
            )
            recovery, read_only, database, version = cursor.fetchone()
    return {
        "name": node["name"],
        "patroni_role": node["role"],
        "pg_is_in_recovery": recovery,
        "transaction_read_only": read_only,
        "database": database,
        "version": version,
    }


def marker_exists(node, marker):
    try:
        with psycopg2.connect(dsn(node), connect_timeout=3) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "select exists (select 1 from admin.replication_markers where marker = %s)",
                    (marker,),
                )
                return cursor.fetchone()[0]
    except psycopg2.Error:
        return False


def wait_for_marker(node, marker, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if marker_exists(node, marker):
            return True
        time.sleep(1)
    raise TimeoutError(f"{marker!r} did not reach {node['name']}")


def main():
    before_leader, before_replica = wait_for_roles()
    before_leader_sql = sql_role(before_leader)
    before_replica_sql = sql_role(before_replica)
    with psycopg2.connect(dsn(before_leader)) as connection:
        with connection.cursor() as cursor:
            cursor.execute("create schema if not exists admin")
            cursor.execute(
                """
                create table if not exists admin.replication_markers (
                    marker text primary key,
                    written_on text not null,
                    written_at timestamptz not null default clock_timestamp()
                )
                """
            )
            cursor.execute("truncate admin.replication_markers")
            cursor.execute(
                """
                insert into admin.replication_markers (marker, written_on)
                values (%s, %s)
                """,
                (BEFORE_MARKER, before_leader["name"]),
            )
    wait_for_marker(before_replica, BEFORE_MARKER)

    rejected = {}
    try:
        with psycopg2.connect(dsn(before_replica)) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    insert into admin.replication_markers (marker, written_on)
                    values (%s, %s)
                    """,
                    (REJECTED_MARKER, before_replica["name"]),
                )
    except psycopg2.Error as error:
        rejected = {
            "sqlstate": error.pgcode,
            "message": error.pgerror.strip(),
        }
    if rejected.get("sqlstate") != "25006":
        raise RuntimeError(f"replica write was not rejected as expected: {rejected}")

    response = requests.post(
        f"{before_leader['api']}/switchover",
        json={
            "leader": before_leader["name"],
            "candidate": before_replica["name"],
        },
        timeout=30,
    )
    response.raise_for_status()

    deadline = time.monotonic() + 90
    while True:
        after_leader, after_replica = wait_for_roles()
        if (
            after_leader["name"] == before_replica["name"]
            and after_replica["name"] == before_leader["name"]
        ):
            break
        if time.monotonic() >= deadline:
            raise TimeoutError("switchover did not exchange Patroni roles")
        time.sleep(2)

    if not marker_exists(after_leader, BEFORE_MARKER):
        raise RuntimeError("old marker is missing on the new leader")
    with psycopg2.connect(dsn(after_leader)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                insert into admin.replication_markers (marker, written_on)
                values (%s, %s)
                """,
                (AFTER_MARKER, after_leader["name"]),
            )
    wait_for_marker(after_replica, AFTER_MARKER)

    with psycopg2.connect(dsn(after_leader)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select application_name, state, sync_state,
                       pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)::bigint
                from pg_stat_replication
                order by application_name
                """
            )
            replication = [
                {
                    "application_name": row[0],
                    "state": row[1],
                    "sync_state": row[2],
                    "lag_bytes": row[3],
                }
                for row in cursor.fetchall()
            ]

    evidence = {
        "before": {
            "leader": before_leader_sql,
            "replica": before_replica_sql,
            "marker": BEFORE_MARKER,
            "marker_visible_on_replica": True,
        },
        "replica_write": {
            **rejected,
            "marker_absent": not marker_exists(after_leader, REJECTED_MARKER),
        },
        "switchover": {
            "http_status": response.status_code,
            "response": response.text,
            "old_leader": before_leader["name"],
            "new_leader": after_leader["name"],
        },
        "after": {
            "leader": sql_role(after_leader),
            "replica": sql_role(after_replica),
            "old_marker_present": marker_exists(after_leader, BEFORE_MARKER),
            "new_marker": AFTER_MARKER,
            "new_marker_visible_on_replica": marker_exists(
                after_replica, AFTER_MARKER
            ),
            "replication": replication,
        },
    }
    output = ROOT / "evidence" / "replication.json"
    output.write_text(
        json.dumps(evidence, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(evidence, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
