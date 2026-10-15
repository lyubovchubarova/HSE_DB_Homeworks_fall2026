import json
import subprocess
import sys
import types
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    import psycopg2  # noqa: F401
except ModuleNotFoundError:
    sys.modules["psycopg2"] = types.SimpleNamespace()

from scripts import publish


class RecordingCursor:
    def __init__(self):
        self.statements = []

    def execute(self, query, params=None):
        self.statements.append((" ".join(query.split()).lower(), params))

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class RecordingConnection:
    def __init__(self):
        self.cursor_instance = RecordingCursor()
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class DbtProjectTest(unittest.TestCase):
    def test_manifest_contains_only_the_simple_vacancy_star(self):
        result = subprocess.run(
            [
                str(ROOT / ".venv/bin/dbt"),
                "parse",
                "--project-dir",
                str(ROOT),
                "--profiles-dir",
                str(ROOT),
                "--no-partial-parse",
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout[-4000:])
        manifest = json.loads((ROOT / "target/manifest.json").read_text())
        models = {
            node["name"]
            for node in manifest["nodes"].values()
            if node["resource_type"] == "model"
        }
        self.assertEqual(
            models,
            {
                "stg_trudvsem_vacancies",
                "dim_date",
                "dim_region",
                "dim_sphere",
                "fct_vacancies",
                "mart_regional_sphere_demand",
            },
        )
        sources = {source["name"] for source in manifest["sources"].values()}
        self.assertEqual(sources, {"trudvsem_vacancies"})

    def test_publish_is_one_transaction_without_quality_markers(self):
        connection = RecordingConnection()

        publish.publish_candidate(connection)

        self.assertTrue(connection.committed)
        self.assertFalse(connection.rolled_back)
        statements = " ".join(
            query for query, _params in connection.cursor_instance.statements
        )
        self.assertIn("create schema if not exists published", statements)
        self.assertIn(
            "create table published.mart_regional_sphere_demand",
            statements,
        )
        self.assertIn(
            "from analytics_candidate.mart_regional_sphere_demand",
            statements,
        )
        self.assertNotIn("quality_marker", statements)
        self.assertNotIn("advisory", statements)


if __name__ == "__main__":
    unittest.main()

