#!/usr/bin/env python3
"""Convert the sanitized HW2 JSONL snapshot to a flat benchmark CSV."""

import argparse
import csv
import gzip
import hashlib
import json
import re
from decimal import Decimal
from pathlib import Path


CSV_COLUMNS = (
    "vacancy_snapshot_key",
    "snapshot_date",
    "vacancy_id",
    "region_code",
    "region_name",
    "federal_district",
    "sphere_name",
    "creation_date",
    "modified_at",
    "job_name",
    "salary_min",
    "salary_max",
    "currency",
    "employment",
    "schedule",
    "education",
    "experience_years",
    "work_places",
    "is_quota",
    "is_special_workplace",
)


def _value(row, name, default=""):
    value = row.get(name)
    return default if value is None or value == "" else value


def _experience_years(value):
    if isinstance(value, int):
        return value
    match = re.search(r"[0-9]+", str(value or ""))
    return int(match.group()) if match else ""


def _work_places(value):
    try:
        return max(int(value), 1)
    except (TypeError, ValueError):
        return 1


def _salary(value):
    if value is None or value == "":
        return None
    return Decimal(str(value))


def build_rows(source_rows):
    result = []
    seen = set()
    for source in source_rows:
        snapshot_date = _value(source, "snapshot_date")
        vacancy_id = _value(source, "vacancy_id")
        if not snapshot_date or not vacancy_id:
            raise ValueError("snapshot_date and vacancy_id are required")
        business_key = (str(snapshot_date), str(vacancy_id))
        if business_key in seen:
            raise ValueError(f"duplicate vacancy snapshot: {business_key}")
        seen.add(business_key)

        salary_min = _salary(source.get("salary_min"))
        salary_max = _salary(source.get("salary_max"))
        if salary_min is not None and salary_max is not None:
            salary_min, salary_max = sorted((salary_min, salary_max))

        key_text = f"{snapshot_date}:{vacancy_id}".encode("utf-8")
        row = {
            "vacancy_snapshot_key": hashlib.sha256(key_text).hexdigest(),
            "snapshot_date": snapshot_date,
            "vacancy_id": vacancy_id,
            "region_code": _value(source, "region_code", "unknown"),
            "region_name": _value(source, "region_name", "Не указано"),
            "federal_district": _value(
                source, "federal_district", "Не указано"
            ),
            "sphere_name": _value(source, "sphere_name", "Не указано"),
            "creation_date": _value(source, "creation_date"),
            "modified_at": _value(source, "modified_at"),
            "job_name": _value(source, "job_name", "Не указано"),
            "salary_min": "" if salary_min is None else salary_min,
            "salary_max": "" if salary_max is None else salary_max,
            "currency": _value(source, "currency"),
            "employment": _value(source, "employment", "Не указано"),
            "schedule": _value(source, "schedule", "Не указано"),
            "education": _value(source, "education", "Не указано"),
            "experience_years": _experience_years(source.get("experience_years")),
            "work_places": _work_places(source.get("work_places")),
            "is_quota": str(bool(source.get("is_quota"))).lower(),
            "is_special_workplace": str(
                bool(source.get("is_special_workplace"))
            ).lower(),
        }
        result.append(row)
    return sorted(result, key=lambda row: row["vacancy_snapshot_key"])


def read_jsonl(path):
    rows = []
    with gzip.open(path, "rt", encoding="utf-8") as source:
        for line in source:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def _write_rows(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def build_csv(source, output, metadata_path, control_output=None):
    rows = build_rows(read_jsonl(source))
    if not rows:
        raise ValueError("snapshot is empty")
    _write_rows(output, rows)
    if control_output is not None:
        _write_rows(control_output, rows[: min(1000, len(rows))])

    snapshot_dates = sorted({row["snapshot_date"] for row in rows})
    if len(snapshot_dates) != 1:
        raise ValueError("source must contain one snapshot date")
    metadata = {
        "source": "Работа России",
        "snapshot_date": snapshot_dates[0],
        "rows": len(rows),
        "regions": len({row["region_code"] for row in rows}),
        "spheres": len({row["sphere_name"] for row in rows}),
        "csv_sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
    }
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return metadata


def main():
    parser = argparse.ArgumentParser(description="Build a flat vacancy CSV")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--control-output", type=Path)
    args = parser.parse_args()

    metadata = build_csv(
        args.input,
        args.output,
        args.metadata,
        control_output=args.control_output,
    )
    print(json.dumps(metadata, ensure_ascii=False))


if __name__ == "__main__":
    main()
