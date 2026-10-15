import csv
import gzip
import importlib.util
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "build_vacancy_csv.py"


def load_builder():
    spec = importlib.util.spec_from_file_location("build_vacancy_csv", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_script(name):
    scripts = ROOT / "scripts"
    sys.path.insert(0, str(scripts))
    try:
        spec = importlib.util.spec_from_file_location(name, scripts / f"{name}.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.pop(0)


class VacancyDataTest(unittest.TestCase):
    def test_builder_creates_flat_unique_rows_without_contacts(self):
        builder = load_builder()
        source_rows = [
            {
                "snapshot_date": "2026-10-06",
                "vacancy_id": "vac-2",
                "region_code": "05",
                "region_name": "Дагестан",
                "federal_district": "Северо-Кавказский",
                "sphere_name": "IT",
                "creation_date": "2026-10-01",
                "modified_at": "2026-10-06T12:00:00+0300",
                "job_name": "Аналитик",
                "salary_min": 90000,
                "salary_max": 70000,
                "currency": "руб.",
                "employment": "Полная",
                "schedule": "Полный день",
                "education": "Высшее",
                "experience_years": 1,
                "work_places": 2,
                "is_quota": False,
                "is_special_workplace": False,
                "contact_person": "не должен попасть",
            },
            {
                "snapshot_date": "2026-10-06",
                "vacancy_id": "vac-1",
                "region_code": "05",
                "region_name": "Дагестан",
                "federal_district": "Северо-Кавказский",
                "sphere_name": None,
                "creation_date": "2026-09-01",
                "modified_at": None,
                "job_name": "Инженер",
                "salary_min": None,
                "salary_max": None,
                "currency": None,
                "employment": None,
                "schedule": None,
                "education": None,
                "experience_years": "от 0",
                "work_places": 0,
                "is_quota": True,
                "is_special_workplace": False,
            },
            {
                "snapshot_date": "2026-10-06",
                "vacancy_id": "vac-3",
                "region_code": "05",
                "region_name": "Дагестан",
                "federal_district": "Северо-Кавказский",
                "sphere_name": "IT",
                "creation_date": "2026-09-01",
                "modified_at": None,
                "job_name": "Разработчик",
                "salary_min": "90000",
                "salary_max": "100000",
                "currency": "руб.",
                "employment": "Полная",
                "schedule": "Полный день",
                "education": None,
                "experience_years": "от 1 до 3",
                "work_places": 1,
                "is_quota": False,
                "is_special_workplace": False,
            },
            {
                "snapshot_date": "2026-10-06",
                "vacancy_id": "vac-4",
                "region_code": "05",
                "region_name": "Дагестан",
                "federal_district": "Северо-Кавказский",
                "sphere_name": "IT",
                "creation_date": "2026-09-01",
                "modified_at": None,
                "job_name": "Стажёр",
                "salary_min": None,
                "salary_max": None,
                "currency": None,
                "employment": "Полная",
                "schedule": "Полный день",
                "education": None,
                "experience_years": "не требуется",
                "work_places": 1,
                "is_quota": False,
                "is_special_workplace": False,
            },
        ]

        rows = builder.build_rows(source_rows)

        self.assertEqual({row["vacancy_id"] for row in rows}, {"vac-1", "vac-2", "vac-3", "vac-4"})
        self.assertEqual(list(rows[0]), list(builder.CSV_COLUMNS))
        self.assertEqual(rows[0]["sphere_name"], "Не указано")
        self.assertEqual(rows[0]["experience_years"], 0)
        self.assertEqual(rows[0]["work_places"], 1)
        by_id = {row["vacancy_id"]: row for row in rows}
        self.assertEqual(by_id["vac-2"]["salary_min"], 70000)
        self.assertEqual(by_id["vac-2"]["salary_max"], 90000)
        self.assertEqual(by_id["vac-3"]["salary_min"], 90000)
        self.assertEqual(by_id["vac-3"]["salary_max"], 100000)
        self.assertEqual(by_id["vac-3"]["experience_years"], 1)
        self.assertEqual(by_id["vac-4"]["experience_years"], "")
        self.assertEqual(len({row["vacancy_snapshot_key"] for row in rows}), 4)
        self.assertNotIn("contact_person", rows[0])

    def test_jsonl_to_csv_round_trip_and_metadata(self):
        builder = load_builder()
        source_row = {
            "snapshot_date": "2026-10-06",
            "vacancy_id": "vac-1",
            "region_code": "05",
            "region_name": "Дагестан",
            "federal_district": "Северо-Кавказский",
            "sphere_name": "IT",
            "creation_date": "2026-09-01",
            "modified_at": "2026-10-06T12:00:00+0300",
            "job_name": "Инженер",
            "salary_min": 60000,
            "salary_max": 80000,
            "currency": "руб.",
            "employment": "Полная",
            "schedule": "Полный день",
            "education": "Высшее",
            "experience_years": 0,
            "work_places": 1,
            "is_quota": False,
            "is_special_workplace": False,
        }
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            source = directory / "snapshot.jsonl.gz"
            output = directory / "vacancies.csv"
            metadata = directory / "metadata.json"
            with gzip.open(source, "wt", encoding="utf-8") as target:
                target.write(json.dumps(source_row, ensure_ascii=False) + "\n")

            result = builder.build_csv(source, output, metadata)

            with output.open(encoding="utf-8", newline="") as handle:
                csv_rows = list(csv.DictReader(handle))
            self.assertEqual(len(csv_rows), 1)
            self.assertEqual(csv_rows[0]["vacancy_id"], "vac-1")
            self.assertEqual(result["rows"], 1)
            self.assertEqual(result["snapshot_date"], "2026-10-06")
            self.assertEqual(json.loads(metadata.read_text()), result)

    def test_database_scripts_use_the_same_csv_contract(self):
        builder = load_builder()
        prepare = load_script("prepare")
        old_pg_bin = os.environ.get("PG_BIN")
        os.environ["PG_BIN"] = "/tmp"
        try:
            backup = load_script("backup_restore")
        finally:
            if old_pg_bin is None:
                os.environ.pop("PG_BIN", None)
            else:
                os.environ["PG_BIN"] = old_pg_bin

        prepared_columns = tuple(prepare.COPY_COLUMNS.replace(",", " ").split())
        self.assertEqual(prepared_columns, builder.CSV_COLUMNS)
        self.assertEqual(backup.EXPECTED_VACANCY_COLUMNS, builder.CSV_COLUMNS)


if __name__ == "__main__":
    unittest.main()
