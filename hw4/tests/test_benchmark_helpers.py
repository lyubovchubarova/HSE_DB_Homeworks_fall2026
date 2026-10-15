import sys
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import benchmark
from scripts.benchmark import summarize
from scripts.queries import PROFILES
from scripts.verify import digest_rows


class BenchmarkHelpersTest(unittest.TestCase):
    def test_digest_is_stable_for_same_rows_in_different_order(self):
        left = [("b", Decimal("2.00")), ("a", None)]
        right = list(reversed(left))

        self.assertEqual(digest_rows(left), digest_rows(right))

    def test_summary_uses_all_five_measured_runs(self):
        summary = summarize([9.0, 5.0, 7.0, 6.0, 8.0])

        self.assertEqual(
            summary,
            {"runs": 5, "median_ms": 7.0, "min_ms": 5.0, "max_ms": 9.0},
        )

    def test_clickhouse_average_raises_decimal_scale_before_division(self):
        for profile in PROFILES:
            with self.subTest(profile=profile.name):
                self.assertIn("toDecimal128", profile.clickhouse)

    def test_clickhouse_date_filter_matches_the_sorting_key_expression(self):
        for profile in PROFILES:
            with self.subTest(profile=profile.name):
                self.assertIn("ifNull(creation_date", profile.clickhouse)

    def test_clickhouse_timer_includes_result_materialization(self):
        class LazyResult:
            summary = {}

            def __init__(self):
                self.consumed = False

            @property
            def result_rows(self):
                self.consumed = True
                return [("done",)]

        result = LazyResult()

        class Client:
            def query(self, _sql):
                return result

        with patch.object(
            benchmark.time,
            "perf_counter",
            side_effect=lambda: 1.0 if result.consumed else 0.0,
        ):
            elapsed, rows, _summary = benchmark._timed_clickhouse(Client(), "select 1")

        self.assertEqual(rows, [("done",)])
        self.assertEqual(elapsed, 1000.0)


if __name__ == "__main__":
    unittest.main()
