#!/usr/bin/env python3
"""Download a current vacancy snapshot from the official Работа России API."""

import argparse
import gzip
import hashlib
import json
import math
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path


API_URL = "https://opendata.trudvsem.ru/api/v1/vacancies/region/{code}"
PAGE_SIZE = 100
MAX_API_ROWS = 10_000
DEFAULT_WORKERS = 8
OUTPUT_DIR = Path("data/source")

REGIONS = (
    {
        "code": "7100000000000",
        "name": "Тульская область",
        "federal_district": "Центральный",
    },
    {
        "code": "3900000000000",
        "name": "Калининградская область",
        "federal_district": "Северо-Западный",
    },
    {
        "code": "3400000000000",
        "name": "Волгоградская область",
        "federal_district": "Южный",
    },
    {
        "code": "0500000000000",
        "name": "Республика Дагестан",
        "federal_district": "Северо-Кавказский",
    },
    {
        "code": "0200000000000",
        "name": "Республика Башкортостан",
        "federal_district": "Приволжский",
    },
    {
        "code": "4500000000000",
        "name": "Курганская область",
        "federal_district": "Уральский",
    },
    {
        "code": "5400000000000",
        "name": "Новосибирская область",
        "federal_district": "Сибирский",
    },
    {
        "code": "2700000000000",
        "name": "Хабаровский край",
        "federal_district": "Дальневосточный",
    },
)


def build_session():
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    retry = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "HSE-DB-Homework/2.0",
            "Accept": "application/json",
        }
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def sanitize_vacancy(vacancy, snapshot_date, federal_district):
    """Project an API vacancy to the small analytical whitelist."""
    region = vacancy.get("region") or {}
    category = vacancy.get("category") or {}
    requirement = vacancy.get("requirement") or {}
    workplace = vacancy.get("workPlaceType") or {}

    vacancy_id = vacancy.get("id")
    if not vacancy_id:
        raise ValueError("vacancy without id")

    return {
        "snapshot_date": str(snapshot_date),
        "vacancy_id": str(vacancy_id),
        "source": vacancy.get("source"),
        "region_code": region.get("region_code"),
        "region_name": region.get("name"),
        "federal_district": federal_district,
        "creation_date": vacancy.get("creation-date"),
        "modified_at": vacancy.get("date_modify"),
        "salary_min": vacancy.get("salary_min"),
        "salary_max": vacancy.get("salary_max"),
        "currency": vacancy.get("currency"),
        "job_name": vacancy.get("job-name"),
        "sphere_name": category.get("specialisation"),
        "employment": vacancy.get("employment"),
        "schedule": vacancy.get("schedule"),
        "education": requirement.get("education"),
        "experience_years": requirement.get("experience"),
        "work_places": vacancy.get("work_places"),
        "is_quota": workplace.get("workPlaceQuota"),
        "is_special_workplace": workplace.get("workPlaceSpecial"),
    }


def _request_page(session, region_code, offset):
    response = session.get(
        API_URL.format(code=region_code),
        params={"offset": offset, "limit": PAGE_SIZE},
        timeout=(15, 90),
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload.get("results", {}).get("vacancies"), list):
        raise ValueError(f"unexpected API response for region {region_code}")
    return payload


def _page_rows(payload, region, snapshot_date):
    return [
        sanitize_vacancy(
            item.get("vacancy", item),
            snapshot_date=snapshot_date,
            federal_district=region["federal_district"],
        )
        for item in payload["results"]["vacancies"]
    ]


def _api_total(payload, region_code):
    total = payload.get("meta", {}).get("total")
    if isinstance(total, bool):
        total = None
    try:
        parsed = int(total)
    except (TypeError, ValueError):
        raise ValueError(
            f"invalid API meta.total for region {region_code}: {total!r}"
        ) from None
    if parsed < 0 or str(total).strip() != str(parsed):
        raise ValueError(
            f"invalid API meta.total for region {region_code}: {total!r}"
        )
    return parsed


def download_region(session, region, snapshot_date, max_workers=DEFAULT_WORKERS):
    """Download one complete region with ordinary numbered pages."""
    first_page = _request_page(session, region["code"], 0)
    total = _api_total(first_page, region["code"])
    if total > MAX_API_ROWS:
        raise ValueError(
            f"{region['name']} has {total} rows; simple API mode supports at most 10,000"
        )

    pages = {0: first_page}
    page_count = math.ceil(total / PAGE_SIZE)
    if page_count > 1:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_request_page, session, region["code"], offset): offset
                for offset in range(1, page_count)
            }
            for future in as_completed(futures):
                pages[futures[future]] = future.result()

    downloaded = []
    for offset in sorted(pages):
        page_total = _api_total(pages[offset], region["code"])
        if page_total != total:
            raise ValueError(
                f"API total changed for {region['name']}: {total} -> {page_total}"
            )
        downloaded.extend(_page_rows(pages[offset], region, snapshot_date))

    if len(downloaded) != total:
        raise ValueError(
            f"incomplete download for {region['name']}: "
            f"downloaded {len(downloaded)} rows, API reported {total}"
        )

    by_id = {}
    for row in downloaded:
        current = by_id.get(row["vacancy_id"])
        if current is None or (row.get("modified_at") or "") >= (
            current.get("modified_at") or ""
        ):
            by_id[row["vacancy_id"]] = row
    if len(by_id) != total:
        raise ValueError(
            f"duplicate vacancy ids for {region['name']}: "
            f"downloaded {total} rows but only {len(by_id)} unique ids"
        )
    return [by_id[vacancy_id] for vacancy_id in sorted(by_id)]


def download(session=None, snapshot_date=None, max_workers=DEFAULT_WORKERS):
    session = session or build_session()
    snapshot_date = snapshot_date or date.today().isoformat()
    rows = []
    for region in REGIONS:
        region_rows = download_region(
            session,
            region,
            snapshot_date=snapshot_date,
            max_workers=max_workers,
        )
        print(f"region={region['name']} vacancies={len(region_rows)}")
        rows.extend(region_rows)

    # A vacancy should belong to one region, but keep the snapshot key unique.
    unique = {row["vacancy_id"]: row for row in rows}
    return [unique[vacancy_id] for vacancy_id in sorted(unique)]


def write_snapshot(path, rows):
    """Write deterministic JSONL into a gzip file and replace the target atomically."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".part",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            with gzip.GzipFile(fileobj=temporary, mode="wb", mtime=0) as compressed:
                for row in rows:
                    line = (
                        json.dumps(
                            row,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                        + "\n"
                    ).encode("utf-8")
                    digest.update(line)
                    compressed.write(line)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_path, path)
    except Exception:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise
    return digest.hexdigest()


def validate_snapshot_date(snapshot_date, today=None):
    today_text = (today or date.today()).isoformat()
    if snapshot_date != today_text:
        raise ValueError(
            f"download labels current API data with the current date {today_text}; "
            f"got {snapshot_date}. Use the saved file to reproduce an older snapshot."
        )
    return snapshot_date


def main():
    parser = argparse.ArgumentParser(description="Download current Работа России vacancies")
    parser.add_argument("--snapshot-date", default=date.today().isoformat())
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    if args.workers < 1:
        parser.error("--workers must be positive")
    validate_snapshot_date(args.snapshot_date)
    output = args.output or (
        OUTPUT_DIR / f"trudvsem_vacancies_{args.snapshot_date}.jsonl.gz"
    )
    rows = download(snapshot_date=args.snapshot_date, max_workers=args.workers)
    checksum = write_snapshot(output, rows)
    print(f"saved={output} vacancies={len(rows)} sha256={checksum}")


if __name__ == "__main__":
    main()
