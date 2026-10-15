import gzip
import json
import sys
import tempfile
import threading
import types
import unittest
from datetime import date
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    import psycopg2  # noqa: F401
except ModuleNotFoundError:
    sys.modules["psycopg2"] = types.SimpleNamespace()

from scripts import download, load_raw


REGION = {
    "code": "0500000000000",
    "name": "Республика Дагестан",
    "federal_district": "Северо-Кавказский",
}


def api_vacancy(vacancy_id, modified="2026-10-06T13:36:08+0300"):
    return {
        "vacancy": {
            "id": vacancy_id,
            "source": "Работа России",
            "region": {
                "region_code": REGION["code"],
                "name": REGION["name"],
            },
            "company": {
                "name": "ООО Тест",
                "inn": "123",
            },
            "creation-date": "2026-10-01",
            "date_modify": modified,
            "salary_min": 50000,
            "salary_max": 70000,
            "currency": "«руб.»",
            "job-name": "Аналитик",
            "category": {"specialisation": "Информационные технологии"},
            "employment": "Полная занятость",
            "schedule": "Полный рабочий день",
            "requirement": {
                "education": "Высшее",
                "experience": 1,
                "qualification": "Этот свободный текст сохранять нельзя",
            },
            "work_places": 2,
            "workPlaceType": {
                "workPlaceOrdinary": True,
                "workPlaceQuota": False,
                "workPlaceSpecial": False,
            },
            "contact_person": "Иван Иванов",
            "contact_list": [{"contact_type": "Телефон", "contact_value": "+7000"}],
            "addresses": {"address": [{"location": "точный адрес"}]},
            "duty": "свободный текст обязанностей",
            "requirements": "свободный текст требований",
        }
    }


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []
        self.lock = threading.Lock()

    def get(self, url, **kwargs):
        offset = kwargs["params"]["offset"]
        with self.lock:
            self.calls.append((url, dict(kwargs["params"])))
        return FakeResponse(self.pages[offset])


def page(total, items):
    return {
        "status": "200",
        "meta": {"total": total, "limit": 100},
        "results": {"vacancies": items},
    }


class DownloadTest(unittest.TestCase):
    def test_sanitizer_keeps_only_flat_analytical_fields(self):
        cleaned = download.sanitize_vacancy(
            api_vacancy("vac-1")["vacancy"],
            snapshot_date="2026-10-06",
            federal_district=REGION["federal_district"],
        )

        self.assertEqual(
            cleaned,
            {
                "snapshot_date": "2026-10-06",
                "vacancy_id": "vac-1",
                "source": "Работа России",
                "region_code": "0500000000000",
                "region_name": "Республика Дагестан",
                "federal_district": "Северо-Кавказский",
                "creation_date": "2026-10-01",
                "modified_at": "2026-10-06T13:36:08+0300",
                "salary_min": 50000,
                "salary_max": 70000,
                "currency": "«руб.»",
                "job_name": "Аналитик",
                "sphere_name": "Информационные технологии",
                "employment": "Полная занятость",
                "schedule": "Полный рабочий день",
                "education": "Высшее",
                "experience_years": 1,
                "work_places": 2,
                "is_quota": False,
                "is_special_workplace": False,
            },
        )
        serialized = json.dumps(cleaned, ensure_ascii=False).lower()
        for forbidden in (
            "contact",
            "телефон",
            "address",
            "иван",
            "qualification",
            "duty",
            "requirements",
        ):
            self.assertNotIn(forbidden, serialized)

    def test_region_download_uses_simple_page_offsets(self):
        first_page_items = [api_vacancy(f"vac-{index}") for index in range(1, 101)]
        pages = {
            0: page(101, first_page_items),
            1: page(101, [api_vacancy("vac-101")]),
        }
        session = FakeSession(pages)

        rows = download.download_region(
            session,
            REGION,
            snapshot_date="2026-10-06",
            max_workers=2,
        )

        self.assertEqual(len(rows), 101)
        self.assertEqual({row["vacancy_id"] for row in rows}, {f"vac-{index}" for index in range(1, 102)})
        self.assertEqual(sorted(call[1]["offset"] for call in session.calls), [0, 1])
        self.assertTrue(all(call[1]["limit"] == 100 for call in session.calls))

    def test_incomplete_region_download_is_rejected(self):
        session = FakeSession(
            {
                0: page(101, [api_vacancy("vac-1")]),
                1: page(101, []),
            }
        )

        with self.assertRaisesRegex(ValueError, "reported 101"):
            download.download_region(
                session,
                REGION,
                snapshot_date="2026-10-06",
                max_workers=2,
            )

    def test_missing_api_total_is_rejected(self):
        payload = page(0, [])
        payload["meta"].pop("total")
        session = FakeSession({0: payload})

        with self.assertRaisesRegex(ValueError, "meta.total"):
            download.download_region(
                session,
                REGION,
                snapshot_date="2026-10-06",
                max_workers=2,
            )

    def test_download_date_must_be_today(self):
        with self.assertRaisesRegex(ValueError, "current date"):
            download.validate_snapshot_date(
                "2026-10-05",
                today=date(2026, 10, 6),
            )

        self.assertEqual(
            download.validate_snapshot_date(
                "2026-10-06",
                today=date(2026, 10, 6),
            ),
            "2026-10-06",
        )

    def test_region_over_api_limit_is_rejected(self):
        session = FakeSession({0: page(10001, [])})

        with self.assertRaisesRegex(ValueError, "10,000"):
            download.download_region(
                session,
                REGION,
                snapshot_date="2026-10-06",
                max_workers=2,
            )

    def test_gzip_jsonl_round_trip(self):
        rows = [
            download.sanitize_vacancy(
                api_vacancy("vac-1")["vacancy"],
                snapshot_date="2026-10-06",
                federal_district=REGION["federal_district"],
            )
        ]
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "snapshot.jsonl.gz"

            checksum = download.write_snapshot(target, rows)

            self.assertEqual(load_raw.read_snapshot(target), rows)
            self.assertEqual(len(checksum), 64)
            self.assertFalse((target.parent / (target.name + ".part")).exists())


class MemoryCursor:
    def __init__(self, connection):
        self.connection = connection

    def execute(self, query, params=None):
        normalized = " ".join(query.lower().split())
        if normalized.startswith("delete from raw.trudvsem_vacancies"):
            snapshot_date = str(params[0])
            self.connection.rows = {
                key: value
                for key, value in self.connection.rows.items()
                if key[0] != snapshot_date
            }

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class MemoryConnection:
    def __init__(self):
        self.rows = {
            ("2026-10-06", "old"): {"vacancy_id": "old"},
            ("2026-10-05", "keep"): {"vacancy_id": "keep"},
        }
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return MemoryCursor(self)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


def memory_execute_values(cursor, _query, values, page_size):
    del page_size
    for snapshot_date, vacancy_id, payload, source_file in values:
        cursor.connection.rows[(str(snapshot_date), vacancy_id)] = {
            "payload": payload,
            "source_file": source_file,
        }


class LoadRawTest(unittest.TestCase):
    def test_reloading_date_replaces_only_that_snapshot(self):
        rows = [
            download.sanitize_vacancy(
                api_vacancy("new")["vacancy"],
                snapshot_date="2026-10-06",
                federal_district=REGION["federal_district"],
            )
        ]
        connection = MemoryConnection()

        with mock.patch.object(load_raw, "execute_values", memory_execute_values):
            loaded = load_raw.load_vacancies(connection, rows, "snapshot.jsonl.gz")

        self.assertEqual(loaded, 1)
        self.assertTrue(connection.committed)
        self.assertIn(("2026-10-05", "keep"), connection.rows)
        self.assertNotIn(("2026-10-06", "old"), connection.rows)
        self.assertIn(("2026-10-06", "new"), connection.rows)

    def test_mixed_snapshot_dates_are_rejected_before_database_write(self):
        rows = [
            {"snapshot_date": "2026-10-06", "vacancy_id": "one"},
            {"snapshot_date": "2026-10-05", "vacancy_id": "two"},
        ]
        connection = MemoryConnection()

        with self.assertRaisesRegex(ValueError, "one snapshot_date"):
            load_raw.load_vacancies(connection, rows, "mixed.jsonl.gz")

        self.assertFalse(connection.committed)


if __name__ == "__main__":
    unittest.main()
