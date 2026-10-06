-- Trino 483 script for the Open-Meteo Iceberg table and a federated lookup.
-- lakehouse is the durable Iceberg catalog; memory is a separate, volatile
-- catalog, so memory.default.month_seasons must be recreated after a restart.

-- 1. Discover the available catalogs and inspect the source table.
SHOW CATALOGS;

SHOW TABLES IN lakehouse.student_01;

DESCRIBE lakehouse.student_01.open_meteo;

SELECT count(*) AS total_observations
FROM lakehouse.student_01.open_meteo;

-- 2. Spark-equivalent control: Moscow local wall-clock year 2023.
-- observed_at is TIMESTAMP without time zone, so these bounds do not convert it.
SELECT
    count(*) AS observations,
    CAST(
        avg(CAST(temperature_c AS decimal(18,6)))
        AS decimal(18,6)
    ) AS average_temperature_c
FROM lakehouse.student_01.open_meteo
WHERE city = 'Москва'
  AND observed_at >= TIMESTAMP '2023-01-01 00:00:00'
  AND observed_at < TIMESTAMP '2024-01-01 00:00:00';

-- 3. Iceberg snapshot history in chronological order (oldest to newest).
SELECT
    snapshot_id,
    committed_at,
    operation,
    CAST(summary['total-records'] AS bigint) AS total_records
FROM lakehouse.student_01."open_meteo$snapshots"
ORDER BY committed_at ASC, snapshot_id ASC;

-- 4. Recreate the volatile month-to-season lookup in the memory catalog.
DROP TABLE IF EXISTS memory.default.month_seasons;

CREATE TABLE memory.default.month_seasons (month_number, season) AS
VALUES
    (1, 'Зима'),
    (2, 'Зима'),
    (3, 'Весна'),
    (4, 'Весна'),
    (5, 'Весна'),
    (6, 'Лето'),
    (7, 'Лето'),
    (8, 'Лето'),
    (9, 'Осень'),
    (10, 'Осень'),
    (11, 'Осень'),
    (12, 'Зима');

SELECT month_number, season
FROM memory.default.month_seasons
ORDER BY month_number;

-- 5a. A valid dictionary returns zero duplicate-key rows.
SELECT month_number, count(*) AS duplicate_count
FROM memory.default.month_seasons
GROUP BY month_number
HAVING count(*) > 1;

-- 5b. A valid dictionary returns zero missing or out-of-domain month rows.
WITH expected_months (month_number) AS (
    SELECT month_number
    FROM UNNEST(sequence(1, 12)) AS expected (month_number)
),
actual_months AS (
    SELECT DISTINCT month_number
    FROM memory.default.month_seasons
)
SELECT
    expected_months.month_number AS missing_month,
    actual_months.month_number AS extra_month
FROM expected_months
FULL OUTER JOIN actual_months
    ON expected_months.month_number = actual_months.month_number
WHERE expected_months.month_number IS NULL
   OR actual_months.month_number IS NULL
ORDER BY COALESCE(expected_months.month_number, actual_months.month_number);

-- 6. Average temperature by local hour, season, and city.
-- LEFT JOIN plus COALESCE keeps observations whose month is not in the lookup.
SELECT
    e.city,
    COALESCE(s.season, 'Не сопоставлен') AS season,
    e.hour,
    count(*) AS observations,
    CAST(
        avg(CAST(e.temperature_c AS decimal(18,6)))
        AS decimal(18,6)
    ) AS average_temperature_c
FROM lakehouse.student_01.open_meteo e
LEFT JOIN memory.default.month_seasons s
    ON e.month = s.month_number
GROUP BY
    e.city,
    COALESCE(s.season, 'Не сопоставлен'),
    e.hour
ORDER BY city, season, hour;

-- 7. Row conservation across the federated LEFT JOIN.
-- With the complete dictionary, before_join = after_join and unmatched = 0.
WITH source AS (
    SELECT month
    FROM lakehouse.student_01.open_meteo
)
SELECT
    (SELECT count(*) FROM source) AS before_join,
    count(*) AS after_join,
    count_if(s.month_number IS NULL) AS unmatched
FROM source e
LEFT JOIN memory.default.month_seasons s
    ON e.month = s.month_number;
