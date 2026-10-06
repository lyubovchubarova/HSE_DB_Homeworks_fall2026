-- Проверяем таблицу в Trino.
SHOW CATALOGS;
SHOW TABLES IN lakehouse.student_01;
DESCRIBE lakehouse.student_01.open_meteo;

SELECT count(*) AS total_observations
FROM lakehouse.student_01.open_meteo;

-- Такой же запрос есть в pipeline.py для Spark.
SELECT
    count(*) AS observations,
    CAST(avg(CAST(temperature_c AS decimal(18,6))) AS decimal(18,6))
        AS average_temperature_c
FROM lakehouse.student_01.open_meteo
WHERE city = 'Москва'
  AND observed_at >= TIMESTAMP '2023-01-01 00:00:00'
  AND observed_at < TIMESTAMP '2024-01-01 00:00:00';

SELECT snapshot_id, committed_at, operation,
       CAST(summary['total-records'] AS bigint) AS total_records
FROM lakehouse.student_01."open_meteo$snapshots"
ORDER BY committed_at;

-- Справочник сезонов для JOIN.
DROP TABLE IF EXISTS memory.default.month_seasons;
CREATE TABLE memory.default.month_seasons (month_number, season) AS
VALUES
    (1, 'Зима'), (2, 'Зима'), (3, 'Весна'), (4, 'Весна'),
    (5, 'Весна'), (6, 'Лето'), (7, 'Лето'), (8, 'Лето'),
    (9, 'Осень'), (10, 'Осень'), (11, 'Осень'), (12, 'Зима');

-- Пустой результат означает, что в справочнике нет дублей.
SELECT month_number, count(*) AS duplicate_count
FROM memory.default.month_seasons
GROUP BY month_number
HAVING count(*) > 1;

-- В какие часы холоднее и теплее в каждом городе и сезоне?
WITH hourly AS (
    SELECT
        e.city,
        COALESCE(s.season, 'Не сопоставлен') AS season,
        e.hour,
        count(*) AS observations,
        CAST(avg(CAST(e.temperature_c AS decimal(18,6))) AS decimal(18,6))
            AS average_temperature_c
    FROM lakehouse.student_01.open_meteo e
    LEFT JOIN memory.default.month_seasons s
        ON e.month = s.month_number
    GROUP BY e.city, COALESCE(s.season, 'Не сопоставлен'), e.hour
),
answer AS (
    SELECT
        city,
        season,
        min_by(hour, average_temperature_c) AS coldest_hour,
        min(average_temperature_c) AS coldest_temperature_c,
        max_by(hour, average_temperature_c) AS warmest_hour,
        max(average_temperature_c) AS warmest_temperature_c,
        CAST(
            max(average_temperature_c) - min(average_temperature_c)
            AS decimal(18,6)
        ) AS daily_amplitude_c
    FROM hourly
    GROUP BY city, season
)
SELECT *
FROM answer
ORDER BY city, season;

-- LEFT JOIN не должен терять строки.
SELECT
    (SELECT count(*) FROM lakehouse.student_01.open_meteo) AS before_join,
    count(*) AS after_join,
    count_if(s.month_number IS NULL) AS unmatched
FROM lakehouse.student_01.open_meteo e
LEFT JOIN memory.default.month_seasons s
    ON e.month = s.month_number;
