import csv
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import data_contract as contract
from scripts import download_new_batch as downloader


def sample_row(snapshot_date="2026-10-06", vacancy_id="vac-1"):
    return {
        "vacancy_snapshot_key": "key-" + vacancy_id,
        "snapshot_date": snapshot_date,
        "vacancy_id": vacancy_id,
        "region_code": "6200000000000",
        "region_name": "Рязанская область",
        "federal_district": "Центральный",
        "sphere_name": "Производство",
        "creation_date": "2026-10-01",
        "modified_at": "2026-10-06T12:00:00+03:00",
        "job_name": "Инженер",
        "salary_min": "50000",
        "salary_max": "70000",
        "currency": "«руб.»",
        "employment": "Полная занятость",
        "schedule": "Полный рабочий день",
        "education": "Высшее",
        "experience_years": "1",
        "work_places": "1",
        "is_quota": "false",
        "is_special_workplace": "false",
    }


def api_row_with_contacts(vacancy_id="vac-1"):
    return {
        "id": vacancy_id,
        "source": "Работа России",
        "region": {
            "region_code": "6200000000000",
            "name": "Рязанская область",
        },
        "creation-date": "2026-10-01",
        "date_modify": "2026-10-06T12:00:00+0300",
        "salary_min": "70000",
        "salary_max": "50000",
        "currency": "«руб.»",
        "job-name": "Инженер",
        "category": {"specialisation": "Производство"},
        "employment": "Полная занятость",
        "schedule": "Полный рабочий день",
        "requirement": {
            "education": "Высшее",
            "experience": "от 1 до 3",
            "qualification": "секретный свободный текст",
        },
        "work_places": 0,
        "workPlaceType": {
            "workPlaceQuota": False,
            "workPlaceSpecial": True,
        },
        "contact_person": "Иван Иванов",
        "contact_list": [{"contact_type": "Телефон", "contact_value": "+7000"}],
        "addresses": {"address": [{"location": "точный адрес"}]},
        "duty": "свободный текст",
        "requirements": "свободный текст",
    }


class DataContractTest(unittest.TestCase):
    def test_duplicate_business_key_between_batches_is_rejected(self):
        row = sample_row()

        with self.assertRaisesRegex(ValueError, "duplicate business key"):
            contract.validate_batches([row], [row.copy()])

    def test_invalid_rows_are_rejected_before_database_use(self):
        bad_places = sample_row(vacancy_id="bad-places")
        bad_places["work_places"] = "0"
        bad_salary = sample_row(vacancy_id="bad-salary")
        bad_salary["salary_min"] = "90000"
        bad_salary["salary_max"] = "80000"

        with self.assertRaisesRegex(ValueError, "work_places"):
            contract.validate_batches([bad_places], [])
        with self.assertRaisesRegex(ValueError, "salary"):
            contract.validate_batches([bad_salary], [])

    def test_csv_round_trip_preserves_exact_column_order(self):
        rows = [sample_row()]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "batch.csv"

            contract.write_csv(path, rows)
            loaded = contract.read_csv(path)

            self.assertEqual(loaded, rows)
            with path.open(encoding="utf-8", newline="") as source:
                self.assertEqual(tuple(csv.DictReader(source).fieldnames), contract.COLUMNS)


class DownloaderTest(unittest.TestCase):
    def test_downloader_runs_as_a_script(self):
        result = subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "download_new_batch.py"), "--help"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )

        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("Download the real HW4 increment", result.stdout)

    def test_missing_total_and_short_download_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "meta.total"):
            downloader.api_total({"meta": {}}, "62")
        with self.assertRaisesRegex(ValueError, "reported 101"):
            downloader.validate_download(101, [sample_row()], "Рязанская область")

    def test_forbidden_fields_never_enter_csv_contract(self):
        cleaned = downloader.sanitize(
            api_row_with_contacts(),
            snapshot_date="2026-10-06",
            federal_district="Центральный",
        )

        self.assertEqual(tuple(cleaned), contract.COLUMNS)
        self.assertEqual(cleaned["salary_min"], "50000")
        self.assertEqual(cleaned["salary_max"], "70000")
        self.assertEqual(cleaned["experience_years"], "1")
        self.assertEqual(cleaned["work_places"], "1")
        serialized = str(cleaned).lower()
        for forbidden in (
            "contact",
            "иван",
            "address",
            "qualification",
            "duty",
            "requirements",
        ):
            self.assertNotIn(forbidden, serialized)

    def test_duplicate_ids_in_download_are_rejected(self):
        rows = [sample_row(), sample_row()]

        with self.assertRaisesRegex(ValueError, "unique"):
            downloader.validate_download(2, rows, "Рязанская область")

    def test_region_scan_is_retried_until_one_pass_is_consistent(self):
        calls = []

        def scan():
            calls.append(1)
            if len(calls) == 1:
                return 2, [sample_row(), sample_row()]
            return 2, [sample_row(), sample_row(vacancy_id="vac-2")]

        rows = downloader.stable_download(scan, "Рязанская область", attempts=2)

        self.assertEqual(len(rows), 2)
        self.assertEqual(len(calls), 2)

    def test_region_scan_fails_after_bounded_retries(self):
        def scan():
            return 2, [sample_row(), sample_row()]

        with self.assertRaisesRegex(ValueError, "after 2 attempts"):
            downloader.stable_download(scan, "Рязанская область", attempts=2)


if __name__ == "__main__":
    unittest.main()
