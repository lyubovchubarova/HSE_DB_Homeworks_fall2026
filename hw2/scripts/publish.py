#!/usr/bin/env python3
"""Publish the tested dbt mart in one PostgreSQL transaction."""

import argparse
import os

import psycopg2


DEFAULT_DSN = "host=127.0.0.1 port=55432 dbname=vacancy_dwh user=postgres"
PUBLIC_RELATION = "mart_regional_sphere_demand"


def publish_candidate(connection):
    try:
        with connection.cursor() as cur:
            cur.execute("create schema if not exists published")
            cur.execute(
                f"drop table if exists published.{PUBLIC_RELATION}"
            )
            cur.execute(
                f"""
                create table published.{PUBLIC_RELATION} as
                select *
                from analytics_candidate.{PUBLIC_RELATION}
                """
            )
            cur.execute(
                f"""
                alter table published.{PUBLIC_RELATION}
                add primary key (snapshot_date, region_code, sphere_name)
                """
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def main():
    parser = argparse.ArgumentParser(description="Publish the vacancy demand mart")
    parser.add_argument("--dsn", default=os.getenv("VACANCY_DWH_DSN", DEFAULT_DSN))
    args = parser.parse_args()
    with psycopg2.connect(args.dsn) as connection:
        publish_candidate(connection)
    print(f"published={PUBLIC_RELATION}")


if __name__ == "__main__":
    main()
