"""Contract tests for the Task 4 SQL and schema deliverables."""

from __future__ import annotations

import re
from pathlib import Path


HW1 = Path(__file__).resolve().parents[1]
QUERIES_PATH = HW1 / "queries.sql"
SCHEMA_PATH = HW1 / "schema.md"
README_PATH = HW1 / "README.md"
REPORT_PATH = HW1 / "report.md"
FORBIDDEN_TEACHING_IDENTIFIERS = ("events_demo", "dwh.events", "price_rub")
EVIDENCE_FILES = (
    "compose-ps.txt",
    "object-listing.txt",
    "spark-result.txt",
    "trino-result.txt",
)
EXPECTED_FIELDS = (
    "location_id",
    "city",
    "timezone",
    "observed_at",
    "observation_date",
    "year",
    "month",
    "hour",
    "temperature_c",
    "precipitation_mm",
    "wind_speed_kmh",
)


def _required_text(path: Path) -> str:
    assert path.exists(), f"required deliverable is missing: {path.relative_to(HW1)}"
    return path.read_text(encoding="utf-8")


def _strip_sql_line_comments(sql: str) -> str:
    return re.sub(r"--[^\n]*(?:\n|$)", "\n", sql)


def _normalized_sql() -> str:
    executable_sql = _strip_sql_line_comments(_required_text(QUERIES_PATH))
    return re.sub(r"\s+", " ", executable_sql.lower()).strip()


def _sql_statement_containing(fragment: str) -> str:
    statements = [
        statement.strip()
        for statement in _normalized_sql().split(";")
        if statement.strip()
    ]
    matches = [statement for statement in statements if fragment in statement]
    assert len(matches) == 1, (
        f"expected one executable SQL statement containing {fragment!r}, "
        f"found {len(matches)}"
    )
    return matches[0]


def _schema_rows(schema: str) -> dict[str, list[str]]:
    rows: dict[str, list[str]] = {}
    for line in schema.splitlines():
        if line.startswith("| `"):
            cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
            rows[cells[0].strip("`")] = cells
    return rows


def _markdown_headings(markdown: str) -> tuple[str, ...]:
    return tuple(
        match.group(1).strip().lower()
        for match in re.finditer(r"^#{1,6}\s+(.+?)\s*$", markdown, re.MULTILINE)
    )


def test_sql_comment_stripping_excludes_non_executable_clauses() -> None:
    sql = "SELECT 1;\n-- LEFT JOIN fake ON comment_only\nSELECT 2;"

    stripped = _strip_sql_line_comments(sql).lower()

    assert "left join" not in stripped
    assert "select 1;" in stripped
    assert "select 2;" in stripped


def test_queries_include_discovery_control_and_snapshot_inspection() -> None:
    sql = _normalized_sql()

    assert "show catalogs" in sql
    assert "show tables in lakehouse.student_01" in sql
    assert "describe lakehouse.student_01.open_meteo" in sql
    assert re.search(
        r"select\s+count\(\*\).*?from\s+lakehouse\.student_01\.open_meteo",
        sql,
    )
    assert "city = 'москва'" in sql
    assert "timestamp '2023-01-01 00:00:00'" in sql
    assert "timestamp '2024-01-01 00:00:00'" in sql
    assert "avg(cast(temperature_c as decimal(18,6)))" in sql
    assert 'lakehouse.student_01."open_meteo$snapshots"' in sql
    for expression in (
        "snapshot_id",
        "committed_at",
        "operation",
        "cast(summary['total-records'] as bigint)",
    ):
        assert expression in sql
    assert "order by committed_at" in sql


def test_queries_create_and_validate_complete_month_dictionary() -> None:
    sql = _normalized_sql()

    assert "drop table if exists memory.default.month_seasons" in sql
    assert "create table memory.default.month_seasons" in sql
    values_match = re.search(
        r"create table memory\.default\.month_seasons.*?as\s+values\s+"
        r"(.*?);",
        sql,
    )
    assert values_match, "month_seasons must be populated by CREATE TABLE AS VALUES"
    rows = re.findall(r"\(\s*(\d{1,2})\s*,\s*'([^']+)'\s*\)", values_match.group(1))
    assert rows == [
        ("1", "зима"),
        ("2", "зима"),
        ("3", "весна"),
        ("4", "весна"),
        ("5", "весна"),
        ("6", "лето"),
        ("7", "лето"),
        ("8", "лето"),
        ("9", "осень"),
        ("10", "осень"),
        ("11", "осень"),
        ("12", "зима"),
    ]
    assert "group by month_number having count(*) > 1" in sql
    coverage = _sql_statement_containing("sequence(1, 12)")
    assert "from unnest(sequence(1, 12))" in coverage
    assert (
        "from expected_months full outer join actual_months "
        "on expected_months.month_number = actual_months.month_number"
    ) in coverage
    assert "expected_months.month_number is null" in coverage
    assert "actual_months.month_number is null" in coverage


def test_queries_preserve_unmatched_rows_and_report_join_conservation() -> None:
    analytical = _sql_statement_containing(
        "coalesce(s.season, 'не сопоставлен') as season"
    )
    assert (
        "from lakehouse.student_01.open_meteo e "
        "left join memory.default.month_seasons s on e.month = s.month_number"
    ) in analytical
    assert (
        "group by e.city, coalesce(s.season, 'не сопоставлен'), e.hour"
    ) in analytical
    assert "count(*) as observations" in analytical
    assert "avg(cast(e.temperature_c as decimal(18,6)))" in analytical
    assert "order by city, season, hour" in analytical

    conservation = _sql_statement_containing("as before_join")
    assert (
        "with source as ( select month "
        "from lakehouse.student_01.open_meteo )"
    ) in conservation
    assert "(select count(*) from source) as before_join" in conservation
    assert "count(*) as after_join" in conservation
    assert "count_if(s.month_number is null) as unmatched" in conservation
    assert (
        "from source e left join memory.default.month_seasons s "
        "on e.month = s.month_number"
    ) in conservation


def test_schema_documents_exact_table_contract_and_storage_layout() -> None:
    schema = _required_text(SCHEMA_PATH)
    lowered = re.sub(r"\s+", " ", schema.lower())
    rows = _schema_rows(schema)

    assert tuple(rows) == EXPECTED_FIELDS
    assert all(len(row) == 6 for row in rows.values())
    assert all(row[4].lower() == "true / optional" for row in rows.values())
    assert all(
        "accepted value must be non-null" in row[5].lower()
        for row in rows.values()
    )
    for type_name in (
        "integer",
        "string",
        "varchar",
        "timestamp_ntz",
        "timestamp(6)",
        "date",
        "double",
    ):
        assert type_name in lowered
    for concept in (
        "open-meteo",
        "timezone=auto",
        "iana",
        "local wall-clock",
        "not a global instant",
        "physical spark schema",
        "nullable=true",
        "iceberg columns are optional",
        "dq accepted-data contract",
        "one city-location/hour",
        "parquet",
        "year",
        "month",
        "iceberg",
        "months(observed_at)",
    ):
        assert concept in lowered
    assert "[-100, 70]" in schema or "[-100,70]" in schema
    assert "0..4" in schema
    assert re.search(r"negative temperature.{0,40}allowed", lowered)
    assert re.search(r"precipitation.{0,80}>=\s*0", lowered)
    assert re.search(r"wind.{0,80}>=\s*0", lowered)
    assert "reanalysis" in lowered
    assert "grid" in lowered


def test_schema_describes_timezone_validation_without_overclaiming() -> None:
    schema = _required_text(SCHEMA_PATH)
    lowered = re.sub(r"\s+", " ", schema.lower())
    timezone_row = _schema_rows(schema)["timezone"]

    assert "expected to be an iana name" in lowered
    assert "not independently check" in lowered
    assert "non-empty" in timezone_row[5].lower()
    assert "valid mapping" not in timezone_row[5].lower()
    assert "configured iana" not in timezone_row[5].lower()


def test_deliverables_do_not_reuse_teaching_dataset_identifiers() -> None:
    combined = (
        _required_text(QUERIES_PATH) + "\n" + _required_text(SCHEMA_PATH)
    ).lower()

    for identifier in FORBIDDEN_TEACHING_IDENTIFIERS:
        assert identifier not in combined


def test_readme_has_reproduction_safety_and_submission_sections() -> None:
    readme = _required_text(README_PATH)
    lowered = readme.lower()
    headings = _markdown_headings(readme)

    for topic in (
        "назначение",
        "структура",
        "требования",
        "учебный этап",
        "воспроизведение",
        "повторный запуск",
        "безопасность",
        "доказательства",
    ):
        assert any(topic in heading for heading in headings), (
            f"README heading must cover {topic!r}"
        )
    for command in (
        "python3 generate_dataset.py --rows 200000 --out data/events_demo.csv",
        "docker-compose exec -T spark spark-submit /scripts/01_upload_csv.py "
        "/data/events_demo.csv",
        "docker-compose exec -T spark spark-submit /scripts/02_csv_to_parquet.py",
        "docker-compose exec -T spark spark-submit /scripts/03_iceberg.py",
        "docker-compose exec -T trino trino < trino/scripts/seminar_demo.sql",
    ):
        assert command in readme
    for concept in (
        "hw1_dir=/absolute/path/to/hw1",
        "infra_dir=/absolute/path/to/student-1/infra",
        "set -o pipefail",
        "docker compose",
        "docker-compose",
        "docker-compose down",
        "down -v",
        "127.0.0.1",
    ):
        assert concept in lowered


def test_report_has_required_analysis_sections_and_evidence_links() -> None:
    report = _required_text(REPORT_PATH)
    lowered = report.lower()
    headings = _markdown_headings(report)

    for topic in (
        "паспорт",
        "raw",
        "схема",
        "качество",
        "формат",
        "партиц",
        "iceberg",
        "spark",
        "федерац",
        "исследователь",
        "ограничения",
        "архитектур",
        "доказательства",
        "чек-лист",
    ):
        assert any(topic in heading for heading in headings), (
            f"report heading must cover {topic!r}"
        )
    for evidence_file in EVIDENCE_FILES:
        assert f"evidence/{evidence_file}" in report
    assert "(schema.md)" in lowered
    assert "open-meteo.com/en/docs/historical-weather-api" in lowered
    assert "open-meteo.com/en/licence" in lowered


def test_submission_docs_have_no_placeholders_or_absolute_user_path() -> None:
    combined = _required_text(README_PATH) + "\n" + _required_text(REPORT_PATH)
    lowered = combined.lower()
    placeholders = ("to" + "do", "tb" + "d", "fix" + "me")

    assert not re.search(
        rf"\b(?:{'|'.join(placeholders)})\b",
        lowered,
    )
    assert "/users/lyubachuba/" not in lowered
