import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "benchmark.py"


def load_module():
    spec = importlib.util.spec_from_file_location("benchmark", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class BenchmarkHelpersTest(unittest.TestCase):
    def test_result_digest_is_stable_and_order_sensitive(self):
        benchmark = load_module()
        first = [("Дагестан", "IT", 2), ("Дагестан", "Строительство", 3)]

        self.assertEqual(benchmark.result_digest(first), benchmark.result_digest(list(first)))
        self.assertNotEqual(
            benchmark.result_digest(first),
            benchmark.result_digest(list(reversed(first))),
        )

    def test_plan_metrics_uses_server_time_and_top_level_buffers(self):
        benchmark = load_module()
        plan = {
            "Planning Time": 0.25,
            "Execution Time": 4.75,
            "Plan": {
                "Node Type": "Aggregate",
                "Actual Rows": 3,
                "Shared Hit Blocks": 7,
                "Shared Read Blocks": 2,
            },
        }

        metrics = benchmark.plan_metrics(plan)

        self.assertEqual(metrics["planning_ms"], 0.25)
        self.assertEqual(metrics["execution_ms"], 4.75)
        self.assertEqual(metrics["result_rows"], 3)
        self.assertEqual(metrics["shared_hit_blocks"], 7)
        self.assertEqual(metrics["shared_read_blocks"], 2)

    def test_fixed_query_uses_vacancy_index_columns(self):
        benchmark = load_module()

        sql = benchmark.query_sql("benchmark.vacancies_base").lower()

        self.assertIn("region_code", sql)
        self.assertIn("sphere_name", sql)
        self.assertIn("creation_date", sql)
        self.assertIn("count(*)::bigint as vacancy_count", sql)
        self.assertNotIn("event", sql)
        self.assertNotIn("session", sql)


if __name__ == "__main__":
    unittest.main()

