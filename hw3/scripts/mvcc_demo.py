#!/usr/bin/env python3
import json
import threading
import time
from pathlib import Path

import psycopg2

from cluster import dsn, wait_for_roles


ROOT = Path(__file__).resolve().parents[1]


def scalar(connection, sql, parameters=None):
    with connection.cursor() as cursor:
        cursor.execute(sql, parameters)
        return cursor.fetchone()[0]


def main():
    leader, _ = wait_for_roles()
    connection_string = dsn(leader)

    with psycopg2.connect(connection_string) as setup:
        with setup.cursor() as cursor:
            cursor.execute(
                """
                create table if not exists admin.mvcc_demo (
                    id integer primary key,
                    value text not null
                )
                """
            )
            cursor.execute(
                """
                insert into admin.mvcc_demo (id, value)
                values (1, 'committed-before')
                on conflict (id) do update set value = excluded.value
                """
            )

    session1 = psycopg2.connect(connection_string)
    session2 = psycopg2.connect(connection_string)
    observer = psycopg2.connect(connection_string)
    try:
        session1.set_session(isolation_level="READ COMMITTED", autocommit=False)
        session2.set_session(isolation_level="READ COMMITTED", autocommit=False)
        session1_pid = scalar(session1, "select pg_backend_pid()")
        session2_pid = scalar(session2, "select pg_backend_pid()")
        isolation1 = scalar(
            session1, "select current_setting('transaction_isolation')"
        )
        isolation2 = scalar(
            session2, "select current_setting('transaction_isolation')"
        )

        before = scalar(session2, "select value from admin.mvcc_demo where id = 1")
        with session1.cursor() as cursor:
            cursor.execute(
                "update admin.mvcc_demo set value = 'uncommitted-from-session-1' where id = 1"
            )
        during = scalar(session2, "select value from admin.mvcc_demo where id = 1")
        session1.commit()
        after = scalar(session2, "select value from admin.mvcc_demo where id = 1")
        session2.commit()

        # Lock demonstration: session 2 waits while session 1 owns the row lock.
        with session1.cursor() as cursor:
            cursor.execute(
                "update admin.mvcc_demo set value = 'lock-holder' where id = 1"
            )

        worker_result = {}

        def blocked_update():
            try:
                with session2.cursor() as cursor:
                    cursor.execute(
                        "update admin.mvcc_demo set value = 'waiting-writer' where id = 1"
                    )
                worker_result["completed"] = True
            except Exception as error:
                worker_result["error"] = str(error)

        worker = threading.Thread(target=blocked_update)
        worker.start()

        lock_state = None
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            with observer.cursor() as cursor:
                cursor.execute(
                    """
                    select wait_event_type, wait_event, pg_blocking_pids(pid)
                    from pg_stat_activity
                    where pid = %s
                    """,
                    (session2_pid,),
                )
                row = cursor.fetchone()
            if row and row[0] == "Lock" and row[2]:
                lock_state = {
                    "waiting_pid": session2_pid,
                    "wait_event_type": row[0],
                    "wait_event": row[1],
                    "blocking_pids": row[2],
                }
                break
            time.sleep(0.1)
        if lock_state is None:
            raise TimeoutError("could not observe the waiting writer")

        session1.rollback()
        worker.join(timeout=10)
        if worker.is_alive() or not worker_result.get("completed"):
            raise RuntimeError(f"blocked update did not resume: {worker_result}")
        session2.rollback()

        evidence = {
            "leader": leader["name"],
            "isolation": {
                "session1": isolation1,
                "session2": isolation2,
            },
            "sessions": {
                "session1_pid": session1_pid,
                "session2_pid": session2_pid,
            },
            "mvcc": {
                "before_update": before,
                "while_session1_uncommitted": during,
                "after_session1_commit": after,
                "uncommitted_value_hidden": during == before,
                "committed_value_visible": after == "uncommitted-from-session-1",
            },
            "lock": {
                **lock_state,
                "session2_resumed_after_session1_rollback": True,
                "both_lock_transactions_finished": True,
            },
        }
    finally:
        session1.close()
        session2.close()
        observer.close()

    with psycopg2.connect(connection_string) as cleanup:
        with cleanup.cursor() as cursor:
            cursor.execute("drop table admin.mvcc_demo")

    output = ROOT / "evidence" / "mvcc.json"
    output.write_text(
        json.dumps(evidence, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(evidence, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
