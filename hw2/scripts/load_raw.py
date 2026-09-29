#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
from pathlib import Path

import psycopg2
from psycopg2.extras import Json


DEFAULT_DSN = "host=127.0.0.1 port=55432 dbname=kudago_dwh user=postgres"


def _validated_events(payload):
    if not isinstance(payload, dict) or not isinstance(payload.get("results"), list):
        raise ValueError("payload must contain a results list")

    for item in payload["results"]:
        if not isinstance(item, dict) or item.get("id") is None:
            raise ValueError("each event must have an id")
        dates = item.get("dates")
        if not isinstance(dates, list) or not dates:
            raise ValueError(f"event {item['id']} must have a non-empty dates list")
        for index, period in enumerate(dates):
            if not isinstance(period, dict) or not isinstance(period.get("start"), int):
                raise ValueError(
                    f"event {item['id']} dates[{index}] must have an integer start"
                )
        yield item


def _canonical_json(item):
    return json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def load_events(
    connection,
    payload,
    source_file,
    source_system="kudago",
    replace_source=False,
):
    events = list(_validated_events(payload))
    changed = 0
    with connection.cursor() as cur:
        cur.execute("create schema if not exists raw")
        cur.execute(
            """
            create table if not exists raw.kudago_events (
                source_system text not null,
                event_id bigint not null,
                payload jsonb not null,
                source_file text not null,
                payload_sha256 text not null,
                loaded_at timestamptz not null default now(),
                primary key (source_system, event_id)
            )
            """
        )
        if replace_source:
            cur.execute(
                "delete from raw.kudago_events where source_system = %s",
                (source_system,),
            )
        for item in events:
            canonical = _canonical_json(item)
            checksum = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
            cur.execute(
                """
                insert into raw.kudago_events (
                    source_system, event_id, payload, source_file, payload_sha256
                ) values (%s, %s, %s, %s, %s)
                on conflict (source_system, event_id) do update
                set payload = excluded.payload,
                    source_file = excluded.source_file,
                    payload_sha256 = excluded.payload_sha256,
                    loaded_at = now()
                where raw.kudago_events.payload_sha256 <> excluded.payload_sha256
                returning 1
                """,
                (
                    source_system,
                    int(item["id"]),
                    Json(item),
                    source_file,
                    checksum,
                ),
            )
            if cur.fetchone() is not None:
                changed += 1
    connection.commit()
    return changed


def main():
    parser = argparse.ArgumentParser(description="Load frozen KudaGo JSON to PostgreSQL")
    parser.add_argument("path", type=Path)
    parser.add_argument("--source-system", default="kudago")
    parser.add_argument(
        "--replace-source",
        action="store_true",
        help="replace all rows for this source with the file snapshot",
    )
    parser.add_argument("--dsn", default=os.getenv("KUDAGO_DWH_DSN", DEFAULT_DSN))
    args = parser.parse_args()

    payload = json.loads(args.path.read_text(encoding="utf-8"))
    with psycopg2.connect(args.dsn) as connection:
        changed = load_events(
            connection,
            payload,
            args.path.name,
            source_system=args.source_system,
            replace_source=args.replace_source,
        )
    print(f"raw rows inserted_or_updated={changed}")


if __name__ == "__main__":
    main()
