"""Unit tests for the Open-Meteo ingestion helpers."""

from urllib.parse import urlencode

import pytest

from hw1.pipeline import build_source_url, normalize_open_meteo_csv


METADATA_HEADER = (
    "location_id,latitude,longitude,elevation,utc_offset_seconds,"
    "timezone,timezone_abbreviation"
)
OBSERVATION_HEADER = (
    "location_id,time,temperature_2m (°C),precipitation (mm),"
    "wind_speed_10m (km/h)"
)
NORMALIZED_HEADER = (
    "location_id,observed_at_raw,temperature_c_raw,"
    "precipitation_mm_raw,wind_speed_kmh_raw"
)


def _valid_csv(observations: list[str] | None = None) -> bytes:
    observation_rows = observations
    if observation_rows is None:
        observation_rows = [
            "0,2025-01-01T00:00,-3.5,0.00,13.5",
            "1,2025-01-01T00:00,-1.0,0.20,12.0",
        ]
    lines = [
        METADATA_HEADER,
        "0,55.7558,37.6173,140,10800,Europe/Moscow,MSK",
        "1,59.9343,30.3351,17,10800,Europe/Moscow,MSK",
        "2,56.8389,60.6057,250,18000,Asia/Yekaterinburg,+05",
        "3,55.0084,82.9357,160,25200,Asia/Novosibirsk,+07",
        "4,43.6028,39.7342,30,10800,Europe/Moscow,MSK",
        "",
        OBSERVATION_HEADER,
        *observation_rows,
    ]
    return ("\n".join(lines) + "\n").encode("utf-8")


def test_build_source_url_is_reproducible() -> None:
    expected_query = urlencode(
        {
            "latitude": "55.7558,59.9343,56.8389,55.0084,43.6028",
            "longitude": "37.6173,30.3351,60.6057,82.9357,39.7342",
            "start_date": "2021-01-01",
            "end_date": "2025-12-31",
            "hourly": "temperature_2m,precipitation,wind_speed_10m",
            "timezone": "auto",
            "format": "csv",
        }
    )

    assert build_source_url() == (
        f"https://archive-api.open-meteo.com/v1/archive?{expected_query}"
    )
    assert "temperature_2m%2Cprecipitation%2Cwind_speed_10m" in expected_query


def test_normalize_open_meteo_csv_extracts_observations() -> None:
    raw = _valid_csv()

    normalized, metadata, observation_count = normalize_open_meteo_csv(raw)

    assert metadata[0]["timezone"] == "Europe/Moscow"
    assert [row["location_id"] for row in metadata] == ["0", "1", "2", "3", "4"]
    assert observation_count == 2
    assert normalized == (
        f"{NORMALIZED_HEADER}\n"
        "0,2025-01-01T00:00,-3.5,0.00,13.5\n"
        "1,2025-01-01T00:00,-1.0,0.20,12.0\n"
    ).encode("utf-8")


def test_normalize_open_meteo_csv_rejects_unknown_layout() -> None:
    with pytest.raises(ValueError, match="Open-Meteo CSV"):
        normalize_open_meteo_csv(b"not,the,expected,file\n")


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        (_valid_csv([]), "observations"),
        (
            _valid_csv(["0,2025-01-01T00:00,-3.5,0.00,13.5,unexpected"]),
            "columns",
        ),
        (_valid_csv().replace(b"4,43.6028", b"5,43.6028"), "location_id"),
    ],
)
def test_normalize_open_meteo_csv_rejects_invalid_sections(
    raw: bytes, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        normalize_open_meteo_csv(raw)


def test_normalize_open_meteo_csv_decodes_strict_utf8() -> None:
    with pytest.raises(UnicodeDecodeError):
        normalize_open_meteo_csv(_valid_csv() + b"\xff")
