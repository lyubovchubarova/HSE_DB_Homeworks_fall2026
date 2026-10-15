#!/usr/bin/env python3

import argparse
import hashlib
import json
import math
import os
import re
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

if __package__:
    from .data_contract import COLUMNS, file_sha256, read_csv, validate_batches, write_csv
else:
    from data_contract import COLUMNS, file_sha256, read_csv, validate_batches, write_csv


ROOT = Path(__file__).resolve().parents[1]
API_URL = "https://opendata.trudvsem.ru/api/v1/vacancies/region/{code}"
PAGE_SIZE = 100
MAX_API_ROWS = 10_000
REGIONS = {
    "6200000000000": ("Рязанская область", "Центральный"),
    "5700000000000": ("Орловская область", "Центральный"),
    "4900000000000": ("Магаданская область", "Дальневосточный"),
}


class InconsistentDownload(ValueError):
    pass


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
    )
    session = requests.Session()
    session.headers.update({"User-Agent": "HSE-DB-HW4/1.0", "Accept": "application/json"})
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def api_total(payload, region_code):
    total = payload.get("meta", {}).get("total")
    if isinstance(total, bool):
        total = None
    try:
        parsed = int(total)
    except (TypeError, ValueError):
        raise ValueError(f"invalid API meta.total for region {region_code}: {total!r}") from None
    if parsed < 0 or str(total).strip() != str(parsed):
        raise ValueError(f"invalid API meta.total for region {region_code}: {total!r}")
    return parsed


def validate_download(reported_total, rows, region_name):
    if len(rows) != reported_total:
        raise InconsistentDownload(
            f"incomplete download for {region_name}: downloaded {len(rows)} rows, "
            f"API reported {reported_total}"
        )
    vacancy_ids = [row["vacancy_id"] for row in rows]
    if len(vacancy_ids) != len(set(vacancy_ids)):
        raise InconsistentDownload(
            f"download for {region_name} does not contain unique vacancy ids"
        )
    return rows


def stable_download(scan, region_name, attempts=4):
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            total, rows = scan()
            return validate_download(total, rows, region_name)
        except InconsistentDownload as error:
            last_error = error
            if attempt < attempts:
                print(f"region={region_name} retry={attempt} reason={error}")
    raise ValueError(
        f"could not download a consistent snapshot for {region_name} "
        f"after {attempts} attempts: {last_error}"
    ) from last_error


def _number(value):
    if value in (None, ""):
        return None
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        raise ValueError(f"invalid salary value: {value!r}") from None
    return format(number, "f")


def _experience(value):
    match = re.search(r"[0-9]+", str(value or ""))
    return match.group() if match else ""


def _text(value, default=""):
    return default if value is None or str(value).strip() == "" else str(value).strip()


def sanitize(vacancy, snapshot_date, federal_district):
    vacancy_id = _text(vacancy.get("id"))
    if not vacancy_id:
        raise ValueError("vacancy without id")
    region = vacancy.get("region") or {}
    category = vacancy.get("category") or {}
    requirement = vacancy.get("requirement") or {}
    workplace = vacancy.get("workPlaceType") or {}
    salary_min = _number(vacancy.get("salary_min"))
    salary_max = _number(vacancy.get("salary_max"))
    if salary_min is not None and salary_max is not None:
        low, high = sorted((Decimal(salary_min), Decimal(salary_max)))
        salary_min, salary_max = format(low, "f"), format(high, "f")
    work_places = vacancy.get("work_places")
    try:
        work_places = max(int(work_places), 1)
    except (TypeError, ValueError):
        work_places = 1
    key = hashlib.sha256(f"{snapshot_date}:{vacancy_id}".encode("utf-8")).hexdigest()
    row = {
        "vacancy_snapshot_key": key,
        "snapshot_date": str(snapshot_date),
        "vacancy_id": vacancy_id,
        "region_code": _text(region.get("region_code"), "unknown"),
        "region_name": _text(region.get("name"), "Не указано"),
        "federal_district": federal_district,
        "sphere_name": _text(category.get("specialisation"), "Не указано"),
        "creation_date": _text(vacancy.get("creation-date")),
        "modified_at": _text(vacancy.get("date_modify")),
        "job_name": _text(vacancy.get("job-name"), "Не указано"),
        "salary_min": "" if salary_min is None else salary_min,
        "salary_max": "" if salary_max is None else salary_max,
        "currency": _text(vacancy.get("currency")),
        "employment": _text(vacancy.get("employment"), "Не указано"),
        "schedule": _text(vacancy.get("schedule"), "Не указано"),
        "education": _text(requirement.get("education"), "Не указано"),
        "experience_years": _experience(requirement.get("experience")),
        "work_places": str(work_places),
        "is_quota": str(bool(workplace.get("workPlaceQuota"))).lower(),
        "is_special_workplace": str(bool(workplace.get("workPlaceSpecial"))).lower(),
    }
    if tuple(row) != COLUMNS:
        raise AssertionError("sanitizer and CSV contract differ")
    return row


def request_page(session, region_code, offset):
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


def _scan_region(region_code, region_name, federal_district, snapshot_date, workers):
    session = build_session()
    first = request_page(session, region_code, 0)
    total = api_total(first, region_code)
    if total > MAX_API_ROWS:
        raise ValueError(f"{region_name} has {total} rows; limit is {MAX_API_ROWS}")
    pages = {0: first}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(request_page, session, region_code, offset): offset
            for offset in range(1, math.ceil(total / PAGE_SIZE))
        }
        for future in as_completed(futures):
            pages[futures[future]] = future.result()
    rows = []
    for offset in sorted(pages):
        if api_total(pages[offset], region_code) != total:
            raise InconsistentDownload(f"API total changed while downloading {region_name}")
        for item in pages[offset]["results"]["vacancies"]:
            rows.append(
                sanitize(
                    item.get("vacancy", item),
                    snapshot_date=snapshot_date,
                    federal_district=federal_district,
                )
            )
    return total, rows


def download_region(region_code, region_name, federal_district, snapshot_date, workers=8):
    return stable_download(
        lambda: _scan_region(
            region_code,
            region_name,
            federal_district,
            snapshot_date,
            workers,
        ),
        region_name,
    )


def download_all(snapshot_date, workers=8):
    rows = []
    counts = {}
    with ThreadPoolExecutor(max_workers=len(REGIONS)) as executor:
        futures = {
            executor.submit(
                download_region,
                code,
                name,
                district,
                snapshot_date,
                workers,
            ): (code, name)
            for code, (name, district) in REGIONS.items()
        }
        for future in as_completed(futures):
            code, name = futures[future]
            region_rows = future.result()
            counts[code] = {"name": name, "rows": len(region_rows)}
            rows.extend(region_rows)
            print(f"region={name} vacancies={len(region_rows)}")
    return sorted(rows, key=lambda row: row["vacancy_snapshot_key"]), counts


def write_json_atomic(path, value):
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
            json.dump(value, target, ensure_ascii=False, indent=2)
            target.write("\n")
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary_path, path)
    except Exception:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise


def main():
    parser = argparse.ArgumentParser(description="Download the real HW4 increment")
    parser.add_argument("--snapshot-date", default=date.today().isoformat())
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--initial", type=Path, default=ROOT / "data" / "vacancies_initial.csv")
    parser.add_argument("--output", type=Path, default=ROOT / "data" / "vacancies_new_batch.csv")
    parser.add_argument("--metadata", type=Path, default=ROOT / "data" / "metadata.json")
    args = parser.parse_args()
    today = date.today().isoformat()
    if args.snapshot_date != today:
        parser.error(
            f"download labels current API data with the current date {today}; "
            "use the saved CSV for an older date"
        )
    if args.workers < 1:
        parser.error("--workers must be positive")
    new_rows, counts = download_all(args.snapshot_date, workers=args.workers)
    write_csv(args.output, new_rows)
    initial_rows = read_csv(args.initial)
    summary = validate_batches(initial_rows, new_rows)
    metadata = {
        "source": "Работа России",
        "snapshot_date": args.snapshot_date,
        **summary,
        "regions": counts,
        "initial_sha256": file_sha256(args.initial),
        "new_batch_sha256": file_sha256(args.output),
    }
    write_json_atomic(args.metadata, metadata)
    print(json.dumps(metadata, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
