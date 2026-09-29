#!/usr/bin/env python3
import argparse
import os

import psycopg2


DEFAULT_DSN = "host=127.0.0.1 port=55432 dbname=kudago_dwh user=postgres"
PUBLIC_COLUMNS = (
    "mart_key, event_date, start_hour, category_slug, place_title, "
    "event_count, free_event_count, total_duration_minutes"
)


def publish_candidate(connection):
    """Atomically replace the published mart with the tested candidate."""
    old_autocommit = connection.autocommit
    connection.autocommit = False
    try:
        with connection.cursor() as cur:
            cur.execute("select pg_advisory_xact_lock(20260923)")
            cur.execute("create schema if not exists analytics_public")
            cur.execute("drop table if exists analytics_public.mart_daily_events_next")
            cur.execute(
                f"""
                create table analytics_public.mart_daily_events_next as
                select {PUBLIC_COLUMNS}
                from analytics_candidate.mart_daily_events
                """
            )
            cur.execute(
                "select count(*) from analytics_public.mart_daily_events_next"
            )
            row_count = cur.fetchone()[0]
            if row_count == 0:
                raise ValueError("candidate mart is empty")
            cur.execute(
                "alter table analytics_public.mart_daily_events_next "
                "add primary key (mart_key)"
            )
            cur.execute("drop table if exists analytics_public.mart_daily_events_old")
            cur.execute(
                "alter table if exists analytics_public.mart_daily_events "
                "rename to mart_daily_events_old"
            )
            cur.execute(
                "alter table analytics_public.mart_daily_events_next "
                "rename to mart_daily_events"
            )
            cur.execute("drop table if exists analytics_public.mart_daily_events_old")
        connection.commit()
        return row_count
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.autocommit = old_autocommit


def main():
    parser = argparse.ArgumentParser(description="Publish the tested candidate mart")
    parser.add_argument("--dsn", default=os.getenv("KUDAGO_DWH_DSN", DEFAULT_DSN))
    args = parser.parse_args()
    with psycopg2.connect(args.dsn) as connection:
        row_count = publish_candidate(connection)
    print(f"published rows={row_count}")


if __name__ == "__main__":
    main()
