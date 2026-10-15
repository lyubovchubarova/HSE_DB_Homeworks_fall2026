#!/usr/bin/env python3

import os
import queue
import threading
import time


PG_HOST = os.getenv("HW4_PG_HOST", "127.0.0.1")
PG_PORT = int(os.getenv("HW4_PG_PORT", "55443"))
PG_USER = os.getenv("HW4_PG_USER", "postgres")
PG_PASSWORD = os.getenv("HW4_PG_PASSWORD", "postgres")
CH_HOST = os.getenv("HW4_CH_HOST", "127.0.0.1")
CH_PORT = int(os.getenv("HW4_CH_PORT", "58123"))
CH_USER = os.getenv("HW4_CH_USER", "default")
CH_PASSWORD = os.getenv("HW4_CH_PASSWORD", "clickhouse")


def pg_connect(database="hw4"):
    import psycopg2

    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=database,
        connect_timeout=3,
        options="-c statement_timeout=3000",
    )


def ch_connect(database="hw4"):
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=database,
        connect_timeout=3,
        send_receive_timeout=3,
    )


def _pg_probe():
    connection = pg_connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute("select 1")
            return cursor.fetchone()[0] == 1
    finally:
        connection.close()


def _ch_probe():
    client = ch_connect()
    try:
        return client.command("select 1") == 1
    finally:
        client.close()


def _safe_probe(probe):
    try:
        return bool(probe())
    except Exception:
        return False


def _probe_with_timeout(probe, timeout):
    results = queue.Queue(maxsize=1)

    def run():
        results.put(_safe_probe(probe))

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        return results.get(timeout=max(0, timeout))
    except queue.Empty:
        return False


def wait_for_services(timeout=120, pg_probe=None, ch_probe=None, sleep=time.sleep):
    pg_probe = pg_probe or _pg_probe
    ch_probe = ch_probe or _ch_probe
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"services were not ready in {timeout}s")
        pg_ready = _probe_with_timeout(pg_probe, min(3, remaining))
        remaining = deadline - time.monotonic()
        ch_ready = (
            _probe_with_timeout(ch_probe, min(3, remaining))
            if remaining > 0
            else False
        )
        if pg_ready and ch_ready:
            return True
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"services were not ready in {timeout}s")
        sleep(min(1, remaining))
