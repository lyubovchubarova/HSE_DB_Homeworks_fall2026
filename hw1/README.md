# Домашняя работа 1

Пайплайн загружает почасовую погоду пяти городов из Open-Meteo за 2021–2025
годы, проверяет данные и записывает их в CSV, Parquet и Iceberg. Итоговая
таблица: `lakehouse.student_01.open_meteo`.

## Файлы

- `pipeline.py` — весь пайплайн Spark;
- `queries.sql` — проверки и аналитический запрос Trino;
- `schema.md` — схема таблицы;
- `report.md` — короткий отчёт;
- `evidence/` — вывод последнего запуска;
- `tests/` — небольшие тесты функций без Spark.

## Как запустить

Нужны Docker, Python 3 и выданная папка `student-1/infra`.

Сначала можно проверить исходный учебный пример:

```bash
cd /absolute/path/to/student-1/infra
docker compose build && docker compose up -d
python3 generate_dataset.py --rows 200000 --out data/events_demo.csv
docker compose exec -T spark spark-submit /scripts/01_upload_csv.py /data/events_demo.csv
docker compose exec -T spark spark-submit /scripts/02_csv_to_parquet.py
docker compose exec -T spark spark-submit /scripts/03_iceberg.py
docker compose exec -T trino trino < trino/scripts/seminar_demo.sql
```

Запуск самостоятельной части:

```bash
HW1_DIR=/absolute/path/to/HSE_DB_Homeworks_fall2026/hw1
INFRA_DIR=/absolute/path/to/student-1/infra

cd "$INFRA_DIR"
docker compose build
docker compose up -d
docker compose ps --all

mkdir -p scripts/hw01
cp "$HW1_DIR/pipeline.py" scripts/hw01/pipeline.py

mkdir -p "$HW1_DIR/evidence"
set -o pipefail
docker compose exec -T spark spark-submit /scripts/hw01/pipeline.py \
  2>&1 | tee "$HW1_DIR/evidence/spark-result.txt"

docker compose exec -T minio \
  mc stat --json local/raw/student_01/open_meteo/ingestion_date=2026-10-06/source.csv \
  | tee "$HW1_DIR/evidence/object-listing.txt"

docker compose exec -T trino trino < "$HW1_DIR/queries.sql" \
  | tee "$HW1_DIR/evidence/trino-result.txt"

docker compose ps --all | tee "$HW1_DIR/evidence/compose-ps.txt"
```

Если установлен старый Compose, нужно заменить `docker compose` на
`docker-compose`.

Локальные тесты:

```bash
cd /absolute/path/to/HSE_DB_Homeworks_fall2026
python3 -m py_compile hw1/pipeline.py
python3 -m pytest hw1/tests -q
```

Ожидаемые контрольные значения последнего запуска:

- 219 120 строк в источнике и итоговой таблице;
- 0 отклонённых строк;
- 131 400 и 87 720 строк в двух Iceberg-записях;
- Spark и Trino для Москвы за 2023 год: 8 760 строк, средняя температура
  `6.319680`;
- после федеративного `LEFT JOIN` число строк не меняется.

Остановка без удаления данных:

```bash
cd "$INFRA_DIR"
docker compose down
```

Каталог Trino `memory` хранится только до перезапуска, поэтому справочник
сезонов каждый раз заново создаётся в `queries.sql`.
