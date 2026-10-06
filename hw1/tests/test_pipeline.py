"""Небольшие тесты частей pipeline, которые не требуют Spark."""

import pytest

from hw1.pipeline import iceberg_filters, prepare_csv, source_url


RAW_CSV = """location_id,latitude,longitude,elevation,utc_offset_seconds,timezone,timezone_abbreviation
0,55.7558,37.6173,140,10800,Europe/Moscow,MSK
1,59.9343,30.3351,17,10800,Europe/Moscow,MSK
2,56.8389,60.6057,250,18000,Asia/Yekaterinburg,+05
3,55.0084,82.9357,160,25200,Asia/Novosibirsk,+07
4,43.6028,39.7342,30,10800,Europe/Moscow,MSK

location_id,time,temperature_2m (°C),precipitation (mm),wind_speed_10m (km/h)
0,2025-01-01T00:00,-3.5,0.0,13.5
1,2025-01-01T00:00,-1.0,0.2,12.0
""".encode()


def test_source_url_fixes_period_and_fields() -> None:
    url = source_url()

    assert "start_date=2021-01-01" in url
    assert "end_date=2025-12-31" in url
    assert "temperature_2m%2Cprecipitation%2Cwind_speed_10m" in url
    assert "timezone=auto" in url


def test_prepare_csv_extracts_observations_and_city_metadata() -> None:
    prepared, cities, count = prepare_csv(RAW_CSV)

    assert count == 2
    assert prepared.decode().splitlines() == [
        "location_id,observed_at_raw,temperature_raw,precipitation_raw,wind_raw",
        "0,2025-01-01T00:00,-3.5,0.0,13.5",
        "1,2025-01-01T00:00,-1.0,0.2,12.0",
    ]
    assert cities[0] == (0, "Москва", "Europe/Moscow")
    assert cities[4] == (4, "Сочи", "Europe/Moscow")


def test_prepare_csv_rejects_another_layout() -> None:
    with pytest.raises(ValueError, match="формат Open-Meteo"):
        prepare_csv(b"not,the,expected,file\n")


def test_iceberg_filters_split_rows_without_overlap() -> None:
    assert iceberg_filters() == (
        "observed_at < TIMESTAMP_NTZ '2024-01-01 00:00:00'",
        "observed_at >= TIMESTAMP_NTZ '2024-01-01 00:00:00'",
    )
