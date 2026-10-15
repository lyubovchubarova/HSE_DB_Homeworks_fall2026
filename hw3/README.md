# ДЗ 3: PostgreSQL на данных вакансий

Работа продолжает [ДЗ 2](../hw2/README.md) и использует тот же текущий снимок
официального API «Работа России». Никакой искусственной пятилетней истории
нет: дата снимка входит в ключ `(snapshot_date, vacancy_id)`.

## Данные

`make download` вызывает downloader из ДЗ 2 и строит три небольших файла:

- `data/source/trudvsem_vacancies_YYYY-MM-DD.jsonl.gz` — очищенный снимок;
- `data/trudvsem_vacancies.csv` — плоские строки для PostgreSQL;
- `data/control_vacancies.csv` — первые 1000 строк для контрольных проверок.

Контакты, ФИО, телефоны, email, адреса и свободные тексты отсутствуют.

## Оптимизация

В PostgreSQL загружаются одинаковые данные:

- `benchmark.vacancies_base` — baseline;
- `benchmark.vacancies_indexed` — B-tree по
  `(region_code, sphere_name, creation_date)`;
- `benchmark.vacancies_partitioned` — секции по `snapshot_date`.

Один и тот же отчёт считает спрос по сферам в Башкортостане. Для каждого
варианта выполняется один прогрев и пять измерений через
`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`. Результаты дополнительно
сравниваются через `EXCEPT ALL` в обе стороны.

## Администрирование

Docker Compose поднимает минимальный учебный кластер:

```text
etcd
 ├── patroni1 / PostgreSQL 14
 └── patroni2 / PostgreSQL 14
```

Сценарий проверяет:

- потоковую репликацию и запрет записи на replica;
- плановый switchover;
- `pg_dump -Fc` и восстановление в `hw3_restore`;
- равенство таблиц после restore;
- видимость изменений и блокировку в двух MVCC-сессиях.

## Запуск

```bash
make setup
make all
```

`make download` получает только сегодняшний снимок. Для повторного запуска на
сохранённом файле, например `2026-10-06`, используется команда без обращения к
API:

```bash
make replay SNAPSHOT_DATE=2026-10-06
```

Отдельные шаги:

```bash
make download
make up
make prepare
make benchmark
make replication
make backup-restore
make mvcc
make equality
make test
make render
```

`make stop` останавливает контейнеры, сохраняя named volumes.
`make destroy` явно удаляет учебный кластер и данные.

Evidence записывается в `evidence/`, а HTML-страницы — в
`.runtime/evidence/`.
