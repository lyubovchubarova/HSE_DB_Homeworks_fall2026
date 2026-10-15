import sys
import unittest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import mart, prepare


def sample_row(vacancy_id="one", salary_min="100", work_places="2"):
    return {
        "vacancy_snapshot_key": "key-" + vacancy_id,
        "snapshot_date": "2026-10-06",
        "vacancy_id": vacancy_id,
        "region_code": "62",
        "region_name": "Рязанская область",
        "federal_district": "Центральный",
        "sphere_name": "Производство",
        "creation_date": "2026-10-01",
        "modified_at": "2026-10-06T12:00:00+03:00",
        "job_name": "Инженер",
        "salary_min": salary_min,
        "salary_max": "200" if salary_min else "",
        "currency": "«руб.»",
        "employment": "Полная занятость",
        "schedule": "Полный рабочий день",
        "education": "Высшее",
        "experience_years": "1",
        "work_places": work_places,
        "is_quota": "false",
        "is_special_workplace": "true",
    }


class PrepareHelpersTest(unittest.TestCase):
    def test_postgres_is_analyzed_after_bulk_load(self):
        class RecordingCursor:
            def __init__(self):
                self.statements = []

            def execute(self, statement):
                self.statements.append(statement)

        cursor = RecordingCursor()

        prepare._analyze_postgres(cursor)

        self.assertEqual(cursor.statements, ["analyze public.vacancies"])

    def test_mart_query_does_not_shadow_source_aggregate_columns(self):
        self.assertNotIn(
            "sum(salary_min_count) as salary_min_count,",
            mart.MART_QUERY,
        )
        self.assertNotIn(
            "sum(salary_min_sum) as salary_min_sum,",
            mart.MART_QUERY,
        )

    def test_mart_average_raises_decimal_scale_before_division(self):
        self.assertIn("toDecimal128", mart.MART_QUERY)
        self.assertIn("toDecimal128", mart.DIRECT_QUERY)

    def test_split_batch_probe_requires_two_partial_rows_and_exact_average(self):
        expected = {
            "vacancy_count": 3,
            "workplace_count": 4,
            "salary_min_sum": "600.00",
            "salary_min_count": 2,
            "average_salary_min": "300.00",
        }

        result = prepare.validate_split_batch_probe(2, (3, 4, Decimal("600.00"), 2, Decimal("300.00")))

        self.assertEqual(result["partial_rows"], 2)
        self.assertEqual(result["result"], expected)
        self.assertTrue(result["matches_expected"])

    def test_mart_comparison_has_a_total_order(self):
        expected_order = (
            "order by snapshot_date, region_code, region_name, "
            "sphere_name, currency"
        )

        self.assertIn(expected_order, mart.MART_QUERY)
        self.assertIn(expected_order, mart.DIRECT_QUERY)

    def test_null_salary_does_not_enter_average_denominator(self):
        rows = [sample_row(), sample_row(vacancy_id="two", salary_min="")]

        totals = mart.aggregate_rows(rows)

        self.assertEqual(totals["vacancy_count"], 2)
        self.assertEqual(totals["workplace_count"], 4)
        self.assertEqual(totals["salary_min_sum"], Decimal("100"))
        self.assertEqual(totals["salary_min_count"], 1)
        self.assertEqual(totals["average_salary_min"], Decimal("100"))

    def test_average_combines_partial_sums_with_their_counts(self):
        summary = mart.summarize_parts(
            [
                {"salary_min_sum": Decimal("100"), "salary_min_count": 1},
                {"salary_min_sum": Decimal("300"), "salary_min_count": 3},
            ]
        )

        self.assertEqual(summary["salary_min_sum"], Decimal("400"))
        self.assertEqual(summary["salary_min_count"], 4)
        self.assertEqual(summary["average_salary_min"], Decimal("100"))

    def test_clickhouse_conversion_uses_dates_numbers_nulls_and_booleans(self):
        row = sample_row()
        row["creation_date"] = ""
        row["salary_max"] = ""
        row["experience_years"] = ""

        converted = prepare.to_clickhouse_row(row)

        self.assertIsInstance(converted[1], date)
        self.assertIsNone(converted[7])
        self.assertIsInstance(converted[8], datetime)
        self.assertEqual(converted[10], Decimal("100"))
        self.assertIsNone(converted[11])
        self.assertIsNone(converted[16])
        self.assertEqual(converted[17], 2)
        self.assertIs(converted[18], False)
        self.assertIs(converted[19], True)


if __name__ == "__main__":
    unittest.main()
