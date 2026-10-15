import os
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class CommandInterfaceTest(unittest.TestCase):
    def run_make(self, target):
        env = dict(os.environ)
        env["SNAPSHOT_DATE"] = "2026-10-06"
        return subprocess.run(
            ["make", "-n", target, "SNAPSHOT_DATE=2026-10-06"],
            cwd=ROOT,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )

    def test_pipeline_dry_run_has_four_simple_steps(self):
        result = self.run_make("pipeline")

        self.assertEqual(result.returncode, 0, result.stdout)
        output = result.stdout.lower()
        self.assertIn("scripts/download.py", output)
        self.assertIn("scripts/load_raw.py", output)
        self.assertIn("dbt build", output)
        self.assertIn("scripts/publish.py", output)
        self.assertNotIn("record_quality", output)
        self.assertNotIn("kudago", output)

    def test_test_target_runs_units_and_dbt_build(self):
        result = self.run_make("test")

        self.assertEqual(result.returncode, 0, result.stdout)
        output = result.stdout.lower()
        self.assertIn("unittest discover", output)
        self.assertIn("dbt build", output)
        self.assertNotIn("seed", output)

    def test_start_initializes_postgres_with_available_utf8_locale(self):
        result = self.run_make("start")

        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("LC_ALL=en_US.UTF-8", result.stdout)
        self.assertIn("LANG=en_US.UTF-8", result.stdout)


if __name__ == "__main__":
    unittest.main()
