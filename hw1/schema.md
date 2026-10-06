# Open-Meteo accepted table schema

## Source and grain

The source is the raw CSV response from the Open-Meteo Historical Weather API.
The request uses `timezone=auto`, so the returned timezone is expected to be an
IANA name. The pipeline requires non-empty API metadata but does not
independently check its syntactic IANA validity.
The accepted table grain is **one city-location/hour**: one hourly observation
for one requested location.

`observed_at` is a **local wall-clock** value. It is interpreted using the IANA
name in `timezone`; by itself it is **not a global instant** and must not be
treated as UTC. This is represented as Spark `TIMESTAMP_NTZ` and Trino
`TIMESTAMP(6)` without time zone.

Open-Meteo historical values come from reanalysis/model grid data. A value
describes the grid cell selected for the requested coordinates, not a guaranteed
measurement at an on-site weather station. Grid resolution and reanalysis model
limitations therefore apply.

## Physical schema and accepted-data contract

The physical Spark schema uses `nullable=true`, and the Iceberg columns are
optional. This metadata-level permissiveness is separate from the DQ
accepted-data contract: only rows with non-null values in all 11 fields are
accepted. “Source” below means copied or renamed from the API payload/metadata;
“derived” means calculated during normalization.

| Field | Spark / Trino type | Source or derived | Meaning and unit | Physical nullability | DQ accepted-data constraints |
|---|---|---|---|---|---|
| `location_id` | INTEGER / INTEGER | Source: API location index | Stable requested location identifier; unitless | true / optional | Accepted value must be non-null; integer in `0..4`; must resolve to exactly one configured city |
| `city` | STRING / VARCHAR | Derived: configured city dimension joined by `location_id` | Russian city name; unitless | true / optional | Accepted value must be non-null; one of Москва, Санкт-Петербург, Екатеринбург, Новосибирск, Сочи |
| `timezone` | STRING / VARCHAR | Source: API metadata produced by `timezone=auto` | Expected IANA timezone used to interpret local wall-clock time | true / optional | Accepted value must be non-null and non-empty; IANA syntax is not independently validated |
| `observed_at` | TIMESTAMP_NTZ / TIMESTAMP(6) | Source: parsed API `time` | Local wall-clock observation timestamp; no global offset | true / optional | Accepted value must be non-null and parseable; range `[2021-01-01 00:00:00, 2026-01-01 00:00:00)` |
| `observation_date` | DATE / DATE | Derived from `observed_at` | Local calendar date | true / optional | Accepted value must be non-null; must equal `CAST(observed_at AS DATE)` and remain in 2021-01-01 through 2025-12-31 |
| `year` | INTEGER / INTEGER | Derived from `observed_at` | Local calendar year; year number | true / optional | Accepted value must be non-null; integer in `2021..2025`; must equal `year(observed_at)` |
| `month` | INTEGER / INTEGER | Derived from `observed_at` | Local calendar month number | true / optional | Accepted value must be non-null; integer in `1..12`; must equal `month(observed_at)` |
| `hour` | INTEGER / INTEGER | Derived from `observed_at` | Local hour of day | true / optional | Accepted value must be non-null; integer in `0..23`; must equal `hour(observed_at)` |
| `temperature_c` | DOUBLE / DOUBLE | Source: renamed API `temperature_2m` | Air temperature at 2 m, °C | true / optional | Accepted value must be non-null and finite within `[-100, 70]`; negative temperature is allowed |
| `precipitation_mm` | DOUBLE / DOUBLE | Source: renamed API `precipitation` | Hourly precipitation, mm | true / optional | Accepted value must be non-null and finite; precipitation must be `>= 0` |
| `wind_speed_kmh` | DOUBLE / DOUBLE | Source: renamed API `wind_speed_10m` | Wind speed at 10 m, km/h | true / optional | Accepted value must be non-null and finite; wind speed must be `>= 0` |

## Physical layout

- The accepted Parquet dataset is partitioned by the explicit `year/month`
  columns.
- The Iceberg table `lakehouse.student_01.open_meteo` uses the transform
  `months(observed_at)`.
