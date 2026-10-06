# Open-Meteo Lakehouse Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Создать в `hw1/` полностью воспроизводимую сдачу ДЗ 1 по загрузке CSV Open-Meteo в Parquet и Iceberg с DQ, измерениями, Spark/Trino SQL и evidence реального запуска.

**Architecture:** Один `pipeline.py` скачивает неизменённый многосекционный CSV Open-Meteo, нормализует табличную секцию, выполняет Spark DQ, пишет accepted/rejected, Parquet и Iceberg двумя порциями. `queries.sql` восстанавливает справочник сезонов в Trino `memory`, проверяет snapshots и выполняет федеративный JOIN с Iceberg.

**Tech Stack:** Python 3, PySpark 3.5.9, Apache Iceberg 1.11.0, MinIO/S3A, PostgreSQL JDBC catalog, Trino 483, pytest, Docker Compose.

---

## Структура файлов

- `hw1/pipeline.py` — URL, download, raw upload, нормализация CSV, DQ, benchmark, Parquet, Iceberg и Spark-результат.
- `hw1/tests/test_pipeline_helpers.py` — unit-тесты URL и нормализации ответа без зависимости от Spark.
- `hw1/queries.sql` — Trino-проверки, snapshots, эквивалентная агрегация и федеративный JOIN.
- `hw1/schema.md` — контракт полей итоговой таблицы.
- `hw1/README.md` — повторяемый запуск через выданный стенд.
- `hw1/report.md` — паспорт, фактические числа, измерения и архитектурный вывод.
- `hw1/evidence/*.txt` — неизменённые текстовые результаты команд.

### Task 1: Чистые функции получения и нормализации CSV

**Files:**
- Create: `hw1/pipeline.py`
- Create: `hw1/tests/test_pipeline_helpers.py`

- [ ] **Step 1: Написать падающие тесты**

```python
from pipeline import build_source_url, normalize_open_meteo_csv


def test_build_source_url_is_reproducible():
    url = build_source_url()
    assert "start_date=2021-01-01" in url
    assert "end_date=2025-12-31" in url
    assert "format=csv" in url
    assert "timezone=auto" in url
    assert "temperature_2m%2Cprecipitation%2Cwind_speed_10m" in url


def test_normalize_open_meteo_csv_extracts_observations():
    raw = b"""location_id,latitude,longitude,elevation,utc_offset_seconds,timezone,timezone_abbreviation
0,55.7,37.6,140,0,GMT,GMT
1,59.9,30.3,17,0,GMT,GMT
2,56.8,60.6,250,0,GMT,GMT
3,55.0,82.9,160,0,GMT,GMT
4,43.6,39.7,30,0,GMT,GMT

location_id,time,temperature_2m (\xc2\xb0C),precipitation (mm),wind_speed_10m (km/h)
0,2025-01-01T00:00,-3.5,0.00,13.5
1,2025-01-01T00:00,-1.0,0.20,12.0
"""
    normalized, metadata, observation_rows = normalize_open_meteo_csv(raw)
    assert len(metadata) == 5
    assert metadata[0]["timezone"] == "GMT"
    assert observation_rows == 2
    assert normalized.decode().splitlines()[0] == (
        "location_id,observed_at_raw,temperature_c_raw,"
        "precipitation_mm_raw,wind_speed_kmh_raw"
    )


def test_normalize_open_meteo_csv_rejects_unknown_layout():
    try:
        normalize_open_meteo_csv(b"not,the,expected,file\n")
    except ValueError as error:
        assert "Open-Meteo CSV" in str(error)
    else:
        raise AssertionError("Expected ValueError")
```

- [ ] **Step 2: Проверить красную фазу**

Run: `cd hw1 && python3 -m pytest tests/test_pipeline_helpers.py -q`

Expected: FAIL, потому что `pipeline.py` и функции ещё не существуют.

- [ ] **Step 3: Реализовать минимальные чистые функции**

`pipeline.py` должен объявить фиксированные `CITIES`, `START_DATE`, `END_DATE`,
`HOURLY_FIELDS`; `build_source_url()` использует `urllib.parse.urlencode`.
`normalize_open_meteo_csv()` декодирует UTF-8, проверяет метаданные для пяти
`location_id`, находит точный заголовок секции наблюдений, заменяет только
заголовок и возвращает `(normalized_bytes, metadata_rows, observation_count)`.
Импорты `pyspark` и `boto3` выполняются внутри функций исполнения, чтобы unit-
тесты запускались обычным Python.

- [ ] **Step 4: Проверить зелёную фазу**

Run: `cd hw1 && python3 -m pytest tests/test_pipeline_helpers.py -q`

Expected: `3 passed`.

- [ ] **Step 5: Зафиксировать изменение**

```bash
git add hw1/pipeline.py hw1/tests/test_pipeline_helpers.py
git commit -m "feat: add reproducible Open-Meteo CSV ingestion helpers"
```

### Task 2: Spark DQ, accepted/rejected, Parquet и измерения

**Files:**
- Modify: `hw1/pipeline.py`

- [ ] **Step 1: Добавить контракт DQ**

Использовать staging-схему из пяти `StringType` полей, чтобы сохранить исходные
значения для reject. Преобразовать их в `IntegerType`, `TimestampType` и
`DoubleType`; добавить `city`, `timezone`, `observation_date`, `year`, `month`,
`hour`. `observed_at` хранит локальное гражданское время города, полученное с
параметром API `timezone=auto`.
Сформировать `reject_reason` для неизвестного location, непарсящегося времени,
температуры вне `[-100, 70]`, отрицательных осадков и ветра. Неизвестные или
нечисловые значения должны попадать в reject, а не исчезать.

- [ ] **Step 2: Добавить проверки результата**

Pipeline останавливается, если источник пуст, отсутствует ключевая колонка,
accepted пуст, `source != accepted + rejected` или типы/строки после Parquet
изменились. В лог вывести долю NULL `temperature_c`, число source/accepted/
rejected, размеры raw/accepted CSV/Parquet и пути объектов.

- [ ] **Step 3: Реализовать записи**

- неизменённый raw:
  `s3://raw/student_01/open_meteo/ingestion_date=2026-10-06/source.csv`;
- normalized CSV:
  `/data/hw1/open_meteo_normalized.csv`;
- accepted CSV:
  `s3a://datalake/student_01/open_meteo/accepted_csv/`;
- rejects:
  `s3a://datalake/student_01/open_meteo/rejects/`;
- Parquet:
  `s3a://datalake/student_01/open_meteo/parquet/`, Snappy, `year/month`.

- [ ] **Step 4: Реализовать benchmark**

Прочитать accepted CSV с явной итоговой схемой и Parquet, выполнить для Москвы
за 2023 год один запрос `count(*)` + `avg(temperature_c)`. Чередовать CSV и
Parquet в трёх запусках, показывать первый замер отдельно, сверять результат
каждого запуска и не называть первый замер холодным.

- [ ] **Step 5: Выполнить статическую проверку**

Run: `python3 -m py_compile hw1/pipeline.py && cd hw1 && python3 -m pytest -q`

Expected: компиляция успешна, `3 passed`.

- [ ] **Step 6: Зафиксировать изменение**

```bash
git add hw1/pipeline.py
git commit -m "feat: add data quality parquet and benchmark pipeline"
```

### Task 3: Две записи Iceberg и Spark-контроль

**Files:**
- Modify: `hw1/pipeline.py`

- [ ] **Step 1: Проверить порции до записи**

Посчитать строки до и после `2024-01-01`, убедиться, что обе порции непустые,
не пересекаются и в сумме равны accepted.

- [ ] **Step 2: Реализовать две записи**

Создать `lakehouse.student_01`, затем выполнить `CREATE OR REPLACE TABLE
lakehouse.student_01.open_meteo USING iceberg PARTITIONED BY
(months(observed_at)) AS SELECT ... WHERE observed_at < TIMESTAMP
'2024-01-01 00:00:00'`. Вторую порцию добавить отдельным `INSERT INTO` с
обратным условием.

- [ ] **Step 3: Проверить snapshots и итог**

После каждой записи вывести новые snapshot ID, число строк и финальную сверку с
Parquet. Выполнить Spark-агрегацию по `city`, `month`, `hour` и вывести
контрольный результат `count(*)` и среднюю температуру для Москвы за 2023 год.

- [ ] **Step 4: Выполнить локальные проверки**

Run: `python3 -m py_compile hw1/pipeline.py && cd hw1 && python3 -m pytest -q`

Expected: компиляция успешна, `3 passed`.

- [ ] **Step 5: Зафиксировать изменение**

```bash
git add hw1/pipeline.py
git commit -m "feat: load Open-Meteo into Iceberg in two snapshots"
```

### Task 4: Trino SQL и схема

**Files:**
- Create: `hw1/queries.sql`
- Create: `hw1/schema.md`

- [ ] **Step 1: Написать SQL-проверки**

`queries.sql` последовательно выполняет:

1. `SHOW CATALOGS` и проверку итогового числа строк;
2. эквивалентный Spark контроль для Москвы за 2023 год с
   `avg(CAST(temperature_c AS decimal(18,6)))`;
3. чтение `lakehouse.student_01."open_meteo$snapshots"`;
4. пересоздание `memory.default.month_seasons` двенадцатью уникальными строками;
5. проверку дублей ключа;
6. федеративный `LEFT JOIN`, агрегацию по городу, сезону и часу;
7. сверку числа строк до/после JOIN и числа `Не сопоставлен`.

- [ ] **Step 2: Описать схему**

`schema.md` фиксирует для одиннадцати итоговых полей Spark/Trino тип, единицу,
происхождение, смысл и nullable. Отдельно описать локальное время и IANA-зону,
производные поля и допустимость отрицательной температуры.

- [ ] **Step 3: Проверить отсутствие учебных имён**

Run:

```bash
rg -n "lakehouse\\.dwh\\.events|events_demo|price_rub" hw1/queries.sql hw1/schema.md
```

Expected: нет совпадений.

- [ ] **Step 4: Зафиксировать изменение**

```bash
git add hw1/queries.sql hw1/schema.md
git commit -m "feat: add Trino federation queries and table schema"
```

### Task 5: Запуск стенда и сбор evidence

**Files:**
- Create: `hw1/evidence/compose-ps.txt`
- Create: `hw1/evidence/object-listing.txt`
- Create: `hw1/evidence/spark-result.txt`
- Create: `hw1/evidence/trino-result.txt`

- [ ] **Step 1: Собрать и запустить выданную инфраструктуру**

Run from `/Users/lyubachuba/Downloads/student-1/infra`:

```bash
docker-compose config --quiet
docker-compose build
docker-compose up -d
docker-compose ps --all
```

Expected: PostgreSQL и MinIO healthy; Spark и Trino running; `minio-init`
завершён с кодом 0.

- [ ] **Step 2: Сохранить compose evidence**

```bash
docker-compose ps --all > /Users/lyubachuba/HSE_DB_Homeworks_fall2026/hw1/evidence/compose-ps.txt
```

- [ ] **Step 3: Смонтировать актуальный pipeline в учебный путь**

```bash
mkdir -p scripts/hw01
cp /Users/lyubachuba/HSE_DB_Homeworks_fall2026/hw1/pipeline.py scripts/hw01/pipeline.py
```

- [ ] **Step 4: Запустить полный Spark pipeline**

```bash
docker-compose exec -T spark spark-submit /scripts/hw01/pipeline.py \
  | tee /Users/lyubachuba/HSE_DB_Homeworks_fall2026/hw1/evidence/spark-result.txt
```

Expected: DQ успешен, accepted > 100000, строки CSV = Parquet = Iceberg,
rejects объяснены, benchmark содержит шесть времён, показаны две новые записи.

- [ ] **Step 5: Сохранить листинг raw**

```bash
docker-compose exec -T minio sh -c \
  'mc alias set local http://localhost:9000 admin hse2026minio >/dev/null &&
   mc stat local/raw/student_01/open_meteo/ingestion_date=2026-10-06/source.csv' \
  > /Users/lyubachuba/HSE_DB_Homeworks_fall2026/hw1/evidence/object-listing.txt
```

- [ ] **Step 6: Выполнить Trino SQL**

```bash
docker-compose exec -T trino trino \
  < /Users/lyubachuba/HSE_DB_Homeworks_fall2026/hw1/queries.sql \
  | tee /Users/lyubachuba/HSE_DB_Homeworks_fall2026/hw1/evidence/trino-result.txt
```

Expected: Spark/Trino контроль совпадает, snapshots видны, ключи справочника
уникальны, число строк до/после JOIN совпадает, unmatched равно нулю.

### Task 6: README, отчёт и финальная приёмка

**Files:**
- Create: `hw1/README.md`
- Create: `hw1/report.md`
- Modify: `hw1/evidence/*.txt` only by rerunning source commands if required

- [ ] **Step 1: Написать README**

Указать требования, `docker compose` и fallback `docker-compose`, запуск из
выданного `infra/`, копирование `pipeline.py`, команды Spark/Trino, ожидаемые
этапы и безопасную остановку без `-v`.

- [ ] **Step 2: Заполнить отчёт только фактическими результатами**

Перенести из evidence дату получения, URL, размеры, source/accepted/rejected,
размеры CSV/Parquet, все шесть времён, числа двух Iceberg-порций, snapshot ID,
Spark/Trino контроль и краткий ответ на исследовательский вопрос. Не заменять
реальные результаты предполагаемыми значениями.

- [ ] **Step 3: Добавить архитектурный вывод**

Написать 5–8 предложений: роль S3, Parquet, Iceberg, Spark, Trino и слабое место
локального стенда. Отдельно отметить, что один запуск не доказывает
универсальное ускорение.

- [ ] **Step 4: Дословно сверить десять критериев PDF**

Проверить source passport, raw, schema/DQ, Parquet, benchmark, two writes,
snapshots, Spark/Trino equality, federation и reproducibility/evidence.

- [ ] **Step 5: Запустить финальные проверки**

```bash
cd /Users/lyubachuba/HSE_DB_Homeworks_fall2026/hw1
python3 -m pytest -q
python3 -m py_compile pipeline.py
test -s evidence/compose-ps.txt
test -s evidence/object-listing.txt
test -s evidence/spark-result.txt
test -s evidence/trino-result.txt
rg -n "TBD|TODO|FIXME" README.md report.md schema.md pipeline.py queries.sql
```

Expected: тесты проходят, Python компилируется, все evidence непустые, маркеры
незавершённости отсутствуют.

- [ ] **Step 6: Зафиксировать завершённую работу**

```bash
git add hw1
git commit -m "docs: complete reproducible hw1 submission"
```
