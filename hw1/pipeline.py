"""Pure helpers for reproducible Open-Meteo ingestion."""

from __future__ import annotations

import csv
from urllib.parse import urlencode


SOURCE_ENDPOINT = "https://archive-api.open-meteo.com/v1/archive"
CITIES = (
    ("Москва", 55.7558, 37.6173),
    ("Санкт-Петербург", 59.9343, 30.3351),
    ("Екатеринбург", 56.8389, 60.6057),
    ("Новосибирск", 55.0084, 82.9357),
    ("Сочи", 43.6028, 39.7342),
)
START_DATE = "2021-01-01"
END_DATE = "2025-12-31"
HOURLY_FIELDS = ("temperature_2m", "precipitation", "wind_speed_10m")

METADATA_HEADER = (
    "location_id",
    "latitude",
    "longitude",
    "elevation",
    "utc_offset_seconds",
    "timezone",
    "timezone_abbreviation",
)
OBSERVATION_HEADER = (
    "location_id",
    "time",
    "temperature_2m (°C)",
    "precipitation (mm)",
    "wind_speed_10m (km/h)",
)
NORMALIZED_HEADER = (
    "location_id",
    "observed_at_raw",
    "temperature_c_raw",
    "precipitation_mm_raw",
    "wind_speed_kmh_raw",
)


def build_source_url() -> str:
    """Build the deterministic Open-Meteo archive request URL."""
    query = urlencode(
        {
            "latitude": ",".join(str(city[1]) for city in CITIES),
            "longitude": ",".join(str(city[2]) for city in CITIES),
            "start_date": START_DATE,
            "end_date": END_DATE,
            "hourly": ",".join(HOURLY_FIELDS),
            "timezone": "auto",
            "format": "csv",
        }
    )
    return f"{SOURCE_ENDPOINT}?{query}"


def _parse_row(line: str, section: str) -> list[str]:
    try:
        return next(csv.reader([line], strict=True))
    except csv.Error as error:
        raise ValueError(f"Invalid Open-Meteo CSV {section}: {error}") from error


def normalize_open_meteo_csv(
    raw_bytes: bytes,
) -> tuple[bytes, list[dict[str, str]], int]:
    """Validate Open-Meteo sections and return a Spark-friendly CSV."""
    text = raw_bytes.decode("utf-8", errors="strict")
    lines = text.splitlines()

    if not lines or tuple(_parse_row(lines[0], "metadata header")) != METADATA_HEADER:
        raise ValueError("Invalid Open-Meteo CSV metadata header")

    metadata_lines = lines[1 : 1 + len(CITIES)]
    if len(metadata_lines) != len(CITIES):
        raise ValueError("Invalid Open-Meteo CSV: expected exactly 5 metadata rows")

    metadata_rows: list[dict[str, str]] = []
    for expected_location_id, line in enumerate(metadata_lines):
        values = _parse_row(line, "metadata row")
        if len(values) != len(METADATA_HEADER):
            raise ValueError(
                "Invalid Open-Meteo CSV metadata columns: "
                f"expected {len(METADATA_HEADER)}, got {len(values)}"
            )
        if values[0] != str(expected_location_id):
            raise ValueError(
                "Invalid Open-Meteo CSV metadata location_id sequence: "
                f"expected {expected_location_id}, got {values[0]!r}"
            )
        metadata_rows.append(dict(zip(METADATA_HEADER, values, strict=True)))

    header_index = 1 + len(CITIES)
    while header_index < len(lines) and not lines[header_index]:
        header_index += 1
    if (
        header_index >= len(lines)
        or tuple(_parse_row(lines[header_index], "observation header"))
        != OBSERVATION_HEADER
    ):
        raise ValueError("Invalid Open-Meteo CSV observation header")

    observation_lines = lines[header_index + 1 :]
    if not observation_lines:
        raise ValueError("Invalid Open-Meteo CSV: observations are empty")

    for line in observation_lines:
        values = _parse_row(line, "observation row")
        if len(values) != len(OBSERVATION_HEADER):
            raise ValueError(
                "Invalid Open-Meteo CSV observation columns: "
                f"expected {len(OBSERVATION_HEADER)}, got {len(values)}"
            )

    normalized_text = "\n".join(
        [",".join(NORMALIZED_HEADER), *observation_lines]
    ) + "\n"
    return normalized_text.encode("utf-8"), metadata_rows, len(observation_lines)
