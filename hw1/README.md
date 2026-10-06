# Домашняя работа 1: локальный lakehouse Open-Meteo

## Назначение

Работа воспроизводит путь реального открытого набора данных через raw CSV,
проверки качества, CSV/Parquet, Iceberg и запросы Spark/Trino. Итоговая таблица
`lakehouse.student_01.open_meteo` содержит почасовую погоду пяти городов за
2021–2025 годы. Исследовательский вопрос, результаты и ограничения описаны в
[отчёте](report.md).

## Структура сдачи

- `pipeline.py` — загрузка Open-Meteo, нормализация, DQ, запись и проверки;
- `schema.md` — явный логический и физический контракт таблицы;
- `queries.sql` — проверочные и федеративные запросы Trino;
- `report.md` — методика, результаты и выводы;
- `evidence/` — четыре текстовых свидетельства текущего запуска;
- `tests/` — модульные и контрактные тесты;
- `.gitignore` — исключение локальных служебных файлов.

Большой исходный набор в репозиторий не включён: он повторно получается
публичным запросом из `pipeline.py`.

## Требования и предоставленная инфраструктура

Нужны Docker Desktop, Python 3 и предоставленная преподавателем папка
`student-1/infra`. Команды сборки, запуска и учебного этапа взяты из инструкции
этой инфраструктуры. В Docker Desktop для этого стенда следует выделить не
меньше 4 CPU и примерно 8 GB памяти.

В новых установках Compose обычно вызывается как `docker compose`. На машине,
где готовилась эта сдача, доступна отдельная команда `docker-compose`, поэтому
в воспроизводимом сценарии ниже используется она. Пользователь с Compose plugin
может везде заменить `docker-compose` на `docker compose`.

## Учебный этап 0 из инструкции преподавателя

Сначала из каталога `infra/` выполняется исходная последовательность команд без
изменений:

```bash
docker compose config --quiet
docker compose build
docker compose up -d
docker compose ps --all
docker compose exec -T trino trino --execute "SELECT 1"
python3 generate_dataset.py --rows 200000 --out data/events_demo.csv
docker compose exec -T spark spark-submit /scripts/01_upload_csv.py /data/events_demo.csv
docker compose exec -T spark spark-submit /scripts/02_csv_to_parquet.py
docker compose exec -T spark spark-submit /scripts/03_iceberg.py
docker compose exec -T trino trino < trino/scripts/seminar_demo.sql
```

На машине с отдельным Compose CLI те же пять содержательных команд этапа
запускаются так:

```bash
python3 generate_dataset.py --rows 200000 --out data/events_demo.csv
docker-compose exec -T spark spark-submit /scripts/01_upload_csv.py /data/events_demo.csv
docker-compose exec -T spark spark-submit /scripts/02_csv_to_parquet.py
docker-compose exec -T spark spark-submit /scripts/03_iceberg.py
docker-compose exec -T trino trino < trino/scripts/seminar_demo.sql
```

Этот этап проверяет выданный стенд. Он использует только учебные имена и не
заменяет самостоятельный пайплайн.

## Воспроизведение самостоятельной части

Задайте абсолютные пути без привязки к расположению этого репозитория:

```bash
HW1_DIR=/absolute/path/to/hw1
INFRA_DIR=/absolute/path/to/student-1/infra

cd "$INFRA_DIR"
docker-compose config --quiet
docker-compose build
docker-compose up -d
docker-compose ps --all
docker-compose exec -T trino trino --execute "SELECT 1"
```

`minio`, `postgres` и `trino` должны стать `healthy`, а `minio-init` — завершиться
с кодом 0. Если последний запрос выполнен слишком рано, посмотрите
`docker-compose logs --tail=60 trino` и повторите его после готовности.

Скопируйте решение в каталог, который уже смонтирован в Spark-контейнер:

```bash
mkdir -p "$INFRA_DIR/scripts/hw01"
cp "$HW1_DIR/pipeline.py" "$INFRA_DIR/scripts/hw01/pipeline.py"
```

Запустите пайплайн и сохраните полный вывод. `pipefail` не позволяет `tee`
скрыть ошибку `spark-submit`.

```bash
mkdir -p "$HW1_DIR/evidence"
set -o pipefail
docker-compose exec -T spark spark-submit /scripts/hw01/pipeline.py \
  2>&1 | tee "$HW1_DIR/evidence/spark-result.txt"
```

После успешной записи проверьте raw-объект по полному ключу:

```bash
docker-compose exec -T minio \
  mc stat --json local/raw/student_01/open_meteo/ingestion_date=2026-10-06/source.csv \
  | tee "$HW1_DIR/evidence/object-listing.txt"
```

Затем выполните весь SQL через stdin. Он заново создаёт непостоянный справочник
`memory.default.month_seasons`.

```bash
docker-compose exec -T trino trino \
  < "$HW1_DIR/queries.sql" \
  | tee "$HW1_DIR/evidence/trino-result.txt"

docker-compose ps --all | tee "$HW1_DIR/evidence/compose-ps.txt"
```

Для локальной проверки кода:

```bash
cd "$(dirname "$HW1_DIR")"
python3 -m py_compile hw1/pipeline.py
python3 -m pytest hw1/tests -q
```

## Ожидаемая проверка текущего запуска

Это контрольные значения именно сохранённого запуска, а не обещание для любой
будущей версии источника:

- raw: 7 317 887 байт, SHA-256
  `be8ccdcf90bfe6d8c8c8cc0f23fd666766d81e42f6baf586946d8908fc3f03cb`;
- source/accepted/rejected: 219 120 / 219 120 / 0 строк;
- Iceberg после двух записей: 131 400, затем 219 120 строк;
- Spark и Trino для Москвы за 2023 год: 8 760 строк и средняя температура
  `6.319680`;
- федеративный LEFT JOIN: 219 120 строк до и после, без несопоставленных строк.

## Повторный запуск

Пайплайн перезаписывает только собственные accepted CSV, rejects, Parquet и
текущее состояние собственной Iceberg-таблицы, после чего делает вторую запись
через `INSERT`. Поэтому каждый полный запуск снова демонстрирует две новые
операции записи, но в истории Iceberg могут сохраниться более ранние снимки.
Нельзя отдельно повторять только `INSERT`: это нарушит ожидаемую сохранность
числа строк. Raw-объект с тем же ключом также заменяется новым ответом API.

Каталог Trino `memory` непостоянен. После перезапуска Trino справочник сезонов
исчезает и восстанавливается полным запуском `queries.sql`.

## Необязательный fallback образов MinIO

Эти команды нужны **только если** указанные в выданном `docker-compose.yml`
теги `minio/minio` или `minio/mc` не скачиваются. Сам выданный compose-файл
редактировать не нужно:

```bash
docker pull pgsty/minio:RELEASE.2026-06-18T00-00-00Z
docker tag pgsty/minio:RELEASE.2026-06-18T00-00-00Z minio/minio:RELEASE.2025-09-07T16-13-09Z
docker pull pgsty/mc:RELEASE.2026-09-16T00-00-00Z
docker tag pgsty/mc:RELEASE.2026-09-16T00-00-00Z minio/mc:latest
```

В таком запуске `docker-compose ps` показывает compose-алиасы `minio/minio` и
`minio/mc`, но фактически локальные образы получены из `pgsty`. Именно этот
fallback применён для сохранённого свидетельства; `pgsty` не является исходным
образом, указанным преподавателем.

## Безопасность и остановка

Стенд доступен только локально: опубликованные порты привязаны к `127.0.0.1`.
Пароли внутри compose учебные; внешние токены, ключи и персональные данные в
файлы работы добавлять нельзя. Обычная остановка сохраняет volumes:

```bash
cd "$INFRA_DIR"
docker-compose down
```

Команда `docker-compose down -v` удаляет данные PostgreSQL и MinIO без
возможности восстановления, поэтому её нельзя использовать, если результаты
нужны. Стенд предназначен для локального обучения и не является конфигурацией
с высокой доступностью.

## Доказательства

- [состояние контейнеров](evidence/compose-ps.txt);
- [raw-объект MinIO](evidence/object-listing.txt);
- [вывод Spark](evidence/spark-result.txt);
- [результаты Trino](evidence/trino-result.txt).
