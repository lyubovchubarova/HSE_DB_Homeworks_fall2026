import unittest
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
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


class SqlContractTest(unittest.TestCase):
    def test_schema_uses_vacancy_key_and_columns(self):
        ddl = (ROOT / "sql" / "01_schema.sql").read_text().lower()
        base = (ROOT / "sql" / "02_benchmark_base.sql").read_text().lower()

        for column in COLUMNS:
            self.assertIn(column, ddl)
            self.assertIn(column, base)
        self.assertIn("vacancy_snapshot_key text primary key", ddl)
        self.assertIn("unique (snapshot_date, vacancy_id)", ddl)
        self.assertNotIn("event_", ddl)
        self.assertNotIn("session_", ddl)

    def test_variants_have_one_btree_and_simple_date_partitions(self):
        sql = (ROOT / "sql" / "03_physical_variants.sql").read_text().lower()

        self.assertIn(
            "on benchmark.vacancies_indexed (region_code, sphere_name, creation_date)",
            sql,
        )
        self.assertIn("partition by range (snapshot_date)", sql)
        self.assertIn("vacancies_partitioned_2026", sql)
        self.assertIn("vacancies_partitioned_default", sql)

    def test_equality_uses_except_all_both_directions(self):
        sql = (ROOT / "sql" / "05_equality.sql").read_text().lower()

        self.assertGreaterEqual(sql.count("except all"), 4)
        self.assertIn("vacancies_base", sql)
        self.assertIn("vacancies_indexed", sql)
        self.assertIn("vacancies_partitioned", sql)

    def test_download_dry_run_builds_vacancy_csv(self):
        result = subprocess.run(
            ["make", "-n", "download", "SNAPSHOT_DATE=2026-10-06"],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )

        self.assertEqual(result.returncode, 0, result.stdout)
        output = result.stdout.lower()
        self.assertIn("trudvsem_vacancies_2026-10-06.jsonl.gz", output)
        self.assertIn("build_vacancy_csv.py", output)
        self.assertNotIn("kudago", output)

    def test_admin_ports_are_local_only_and_etcd_is_not_published(self):
        compose = (ROOT / "docker-compose.yml").read_text()

        self.assertNotIn('"2379:2379"', compose)
        for mapping in (
            "127.0.0.1:55441:5432",
            "127.0.0.1:8008:8008",
            "127.0.0.1:55442:5432",
            "127.0.0.1:8009:8008",
        ):
            self.assertIn(mapping, compose)


if __name__ == "__main__":
    unittest.main()
