import unittest
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.render_evidence import plan_summary


class CommandContractTest(unittest.TestCase):
    def test_plan_screenshot_summary_keeps_read_metrics_without_long_condition(self):
        plan = """Keys:\n  region_code\nCondition: very long\nParts: 1/2\nGranules: 1/8\nSearch Algorithm: binary search"""

        summary = plan_summary(plan)

        self.assertIn("Keys:", summary)
        self.assertIn("Granules: 1/8", summary)
        self.assertIn("Search Algorithm: binary search", summary)
        self.assertNotIn("Condition", summary)

    def test_all_target_is_simple_and_stop_is_non_destructive(self):
        makefile = (ROOT / "Makefile").read_text(encoding="utf-8")

        self.assertIn("all: test up prepare verify benchmark render", makefile)
        self.assertIn("docker-compose stop", makefile)
        stop_rule = makefile.split("stop:", 1)[1].split("destroy:", 1)[0]
        self.assertNotIn("down -v", stop_rule)


if __name__ == "__main__":
    unittest.main()
