import json
import subprocess
import sys
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import connections


class InfrastructureContractTest(unittest.TestCase):
    def test_clickhouse_sorting_keys_do_not_use_nullable_date_directly(self):
        ddl = (ROOT / "sql" / "clickhouse.sql").read_text(encoding="utf-8")

        self.assertGreaterEqual(
            ddl.count("ifNull(creation_date, toDate('1970-01-01'))"),
            2,
        )

    def test_compose_renders_local_ports_and_persistent_volumes(self):
        result = subprocess.run(
            ["docker-compose", "config", "--format", "json"],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )

        self.assertEqual(result.returncode, 0, result.stdout)
        config = json.loads(result.stdout)
        self.assertEqual(set(config["services"]), {"postgres", "clickhouse"})
        published = {
            (port.get("host_ip"), int(port["published"]), int(port["target"]))
            for service in config["services"].values()
            for port in service.get("ports", [])
        }
        self.assertEqual(
            published,
            {
                ("127.0.0.1", 55443, 5432),
                ("127.0.0.1", 58123, 8123),
                ("127.0.0.1", 59000, 9000),
            },
        )
        self.assertEqual(set(config["volumes"]), {"postgres_data", "clickhouse_data"})

    def test_readiness_wait_has_a_bounded_timeout(self):
        with self.assertRaisesRegex(
            TimeoutError,
            "services were not ready in 0s",
        ):
            connections.wait_for_services(
                timeout=0,
                pg_probe=lambda: False,
                ch_probe=lambda: False,
                sleep=lambda _seconds: None,
            )

    def test_readiness_timeout_bounds_a_blocking_probe(self):
        started = time.monotonic()

        with self.assertRaisesRegex(TimeoutError, "services were not ready"):
            connections.wait_for_services(
                timeout=0.02,
                pg_probe=lambda: time.sleep(0.25),
                ch_probe=lambda: True,
                sleep=lambda _seconds: None,
            )

        self.assertLess(time.monotonic() - started, 0.15)


if __name__ == "__main__":
    unittest.main()
