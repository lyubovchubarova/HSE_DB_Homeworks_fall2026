#!/usr/bin/env python3

import csv
import hashlib
import os
import tempfile
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path


COLUMNS = (
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

REQUIRED = (
    "vacancy_snapshot_key",
    "snapshot_date",
    "vacancy_id",
    "region_code",
    "region_name",
    "federal_district",
    "sphere_name",
    "job_name",
    "employment",
    "schedule",
    "education",
    "work_places",
    "is_quota",
    "is_special_workplace",
)


def read_csv(path):
    path = Path(path)
    with path.open(encoding="utf-8", newline="") as source:
        reader = csv.DictReader(source)
        if tuple(reader.fieldnames or ()) != COLUMNS:
            raise ValueError(f"unexpected CSV columns in {path}")
        return list(reader)


def write_csv(path, rows):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".part",
            delete=False,
        ) as target:
            temporary_path = Path(target.name)
            writer = csv.DictWriter(target, fieldnames=COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary_path, path)
    except Exception:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _decimal(value, field, vacancy_id):
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation:
        raise ValueError(f"invalid {field} for {vacancy_id}") from None


def _validate_rows(rows, label):
    if not rows:
        raise ValueError(f"{label} batch is empty")
    for row in rows:
        if tuple(row) != COLUMNS:
            raise ValueError(f"unexpected fields in {label} batch")
        vacancy_id = row.get("vacancy_id", "<missing>")
        for field in REQUIRED:
            if row.get(field) in (None, ""):
                raise ValueError(f"missing {field} for {vacancy_id}")
        try:
            date.fromisoformat(str(row["snapshot_date"]))
        except ValueError:
            raise ValueError(f"invalid snapshot_date for {vacancy_id}") from None
        try:
            work_places = int(row["work_places"])
        except (TypeError, ValueError):
            raise ValueError(f"invalid work_places for {vacancy_id}") from None
        if work_places < 1:
            raise ValueError(f"work_places must be positive for {vacancy_id}")
        salary_min = _decimal(row.get("salary_min"), "salary_min", vacancy_id)
        salary_max = _decimal(row.get("salary_max"), "salary_max", vacancy_id)
        if salary_min is not None and salary_max is not None and salary_min > salary_max:
            raise ValueError(f"invalid salary range for {vacancy_id}")


def validate_batches(initial_rows, new_rows):
    initial_rows = list(initial_rows)
    new_rows = list(new_rows)
    _validate_rows(initial_rows, "initial")
    _validate_rows(new_rows, "new")
    combined = initial_rows + new_rows
    keys = [(row["snapshot_date"], row["vacancy_id"]) for row in combined]
    if len(keys) != len(set(keys)):
        raise ValueError("duplicate business key across batches")
    snapshot_dates = sorted({row["snapshot_date"] for row in combined})
    return {
        "initial_rows": len(initial_rows),
        "new_rows": len(new_rows),
        "total_rows": len(combined),
        "unique_business_keys": len(set(keys)),
        "snapshot_dates": snapshot_dates,
    }
