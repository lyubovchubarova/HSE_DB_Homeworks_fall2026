#!/usr/bin/env python3

import json
import os
import subprocess
import textwrap
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

if __package__:
    from .connections import ch_connect, pg_connect, wait_for_services
else:
    from connections import ch_connect, pg_connect, wait_for_services


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "evidence" / "screenshots"


def _font(size):
    candidates = (
        "/System/Library/Fonts/Menlo.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
    )
    for path in candidates:
        if Path(path).exists():
            return ImageFont.truetype(path, size=size)
    return ImageFont.load_default()


def _lines(text, width=100):
    result = []
    for line in str(text).splitlines():
        result.extend(textwrap.wrap(line.strip(), width=92) or [""])
    return result


def draw_page(path, title, sections):
    image = Image.new("RGB", (1600, 900), "#f7f7f2")
    draw = ImageDraw.Draw(image)
    title_font = _font(34)
    body_font = _font(21)
    small_font = _font(17)
    draw.rounded_rectangle((45, 35, 1555, 865), radius=24, fill="white", outline="#d6d6cf", width=2)
    draw.text((85, 72), title, font=title_font, fill="#1d3557")
    y = 140
    for heading, body in sections:
        draw.text((85, y), heading, font=body_font, fill="#2a9d8f")
        y += 38
        for line in _lines(body):
            if y > 820:
                raise ValueError(f"text does not fit in {path.name}")
            draw.text((105, y), line, font=small_font, fill="#252525")
            y += 25
        y += 18
    path.parent.mkdir(parents=True, exist_ok=True)
    image.save(path)


def _read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def plan_summary(plan):
    keys = []
    metrics = []
    in_keys = False
    for raw_line in str(plan).splitlines():
        line = raw_line.strip()
        if line == "Keys:":
            in_keys = True
        elif in_keys and line and not line.startswith("Condition:"):
            keys.append(line)
        else:
            in_keys = False
            if line.startswith(("Parts:", "Granules:", "Search Algorithm:")):
                metrics.append(line)
    kept = [f"Keys: {', '.join(keys)}"] if keys else []
    return "\n".join(kept + metrics)


def _environment():
    wait_for_services()
    pg_connection = pg_connect()
    ch_client = ch_connect()
    try:
        with pg_connection.cursor() as cursor:
            cursor.execute("select version()")
            postgres_version = cursor.fetchone()[0].split(",")[0]
            cursor.execute(
                """
                select name, setting from pg_settings
                where name in ('max_parallel_workers', 'max_parallel_workers_per_gather')
                order by name
                """
            )
            postgres_settings = [f"{name}={value}" for name, value in cursor.fetchall()]
        clickhouse_version = ch_client.command("select version()")
        clickhouse_settings = ch_client.query(
            """
            select name, value from system.settings
            where name in ('max_threads', 'use_query_cache')
            order by name
            """
        ).result_rows
    finally:
        pg_connection.close()
        ch_client.close()
    env = os.environ.copy()
    result = subprocess.run(
        ["docker-compose", "ps", "--format", "json"],
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=True,
    )
    services = []
    for line in result.stdout.splitlines():
        if line.strip():
            item = json.loads(line)
            services.append(f"{item['Service']}: {item['State']} ({item.get('Health', 'n/a')})")
    settings = postgres_settings + [f"{name}={value}" for name, value in clickhouse_settings]
    return postgres_version, clickhouse_version, services, settings


def render_all():
    load = _read_json(ROOT / "evidence" / "load.json")
    mart = _read_json(ROOT / "evidence" / "mart_increment.json")
    integrity = _read_json(ROOT / "evidence" / "integrity.json")
    benchmark = _read_json(ROOT / "evidence" / "benchmark_summary.json")
    postgres_version, clickhouse_version, services, settings = _environment()

    draw_page(
        OUTPUT / "environment.png",
        "HW4: локальное окружение",
        [
            ("Команда", "$ docker-compose ps"),
            ("Версии", f"{postgres_version}\nClickHouse {clickhouse_version}\nPython: текущая .venv"),
            ("Контейнеры", "\n".join(services)),
            ("Параллелизм и кэш", "\n".join(settings)),
            ("Данные", f"initial: {load['contract']['initial_rows']:,}\nnew batch: {load['contract']['new_rows']:,}\ntotal: {load['contract']['total_rows']:,}"),
        ],
    )
    plan_region = (ROOT / "evidence" / "plans" / "clickhouse_region_selective.txt").read_text(encoding="utf-8")
    plan_date = (ROOT / "evidence" / "plans" / "clickhouse_date_selective.txt").read_text(encoding="utf-8")
    draw_page(
        OUTPUT / "plans.png",
        "Selective query: два ORDER BY",
        [
            ("SQL", "SELECT creation_date, count(), sum(work_places), avg(salary_min)\nFROM <table>\nWHERE region_code='3400000000000' AND sphere_name='Здравоохранение...'\n  AND creation_date BETWEEN '2026-09-01' AND '2026-10-06'\nGROUP BY creation_date"),
            ("vacancies_by_region: EXPLAIN indexes=1", plan_summary(plan_region)),
            ("vacancies_by_date: EXPLAIN indexes=1", plan_summary(plan_date)),
            ("Медиана", f"region: {benchmark['selective']['systems']['clickhouse_region']['median_ms']} ms\ndate: {benchmark['selective']['systems']['clickhouse_date']['median_ms']} ms"),
        ],
    )
    raw = integrity["raw"]
    draw_page(
        OUTPUT / "equality.png",
        "Полная проверка переноса",
        [
            ("Команда", "$ .venv/bin/python scripts/verify.py"),
            ("Итог", f"all_equal = {integrity['all_equal']}\nraw all_equal = {raw['all_equal']}"),
            ("Строки и ключи", "\n".join(f"{name}: rows={value['rows']:,}, unique={value['unique_keys']:,}" for name, value in raw['stats'].items())),
            ("SHA-256 всех 20 полей", "\n".join(f"{name}: {digest}" for name, digest in raw['digests'].items())),
        ],
    )
    draw_page(
        OUTPUT / "mart_increment.png",
        "Incremental materialized view",
        [
            ("Команда и вставки", "$ .venv/bin/python scripts/prepare.py\nINSERT initial INTO hw4.vacancy_events\nINSERT new_batch INTO hw4.vacancy_events"),
            ("Initial batch", f"groups: {mart['initial_groups']}\nrows: {mart['initial_totals']['vacancy_count']:,}\nmatches direct: {mart['initial_matches_direct']}"),
            ("После new batch", f"groups: {mart['final_groups']}\nrows: {mart['final_totals']['vacancy_count']:,}\nadded: {load['new_rows_added']:,}\nmatches direct: {mart['final_matches_direct']}"),
            ("Контрольные SHA-256", f"initial: {mart['initial_digest']}\nfinal:   {mart['final_digest']}"),
            ("Две вставки в одну группу", f"partial rows: {mart['split_batch_probe']['partial_rows']}\naverage: {mart['split_batch_probe']['result']['average_salary_min']}\nmatches expected: {mart['split_batch_probe']['matches_expected']}"),
        ],
    )
    print(f"rendered 4 screenshots in {OUTPUT}")


if __name__ == "__main__":
    render_all()
