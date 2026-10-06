# Схема таблицы

Одна строка — одно почасовое наблюдение для одного города. `observed_at` хранит
местное время без часового пояса, а его IANA-зона лежит в `timezone`.

| Поле | Тип Spark / Trino | Смысл |
|---|---|---|
| `location_id` | INT / INTEGER | номер города в запросе |
| `city` | STRING / VARCHAR | название города |
| `timezone` | STRING / VARCHAR | часовой пояс города |
| `observed_at` | TIMESTAMP_NTZ / TIMESTAMP | местные дата и время наблюдения |
| `observation_date` | DATE / DATE | местная дата |
| `year` | INT / INTEGER | год |
| `month` | INT / INTEGER | месяц от 1 до 12 |
| `hour` | INT / INTEGER | час от 0 до 23 |
| `temperature_c` | DOUBLE / DOUBLE | температура на высоте 2 м, °C |
| `precipitation_mm` | DOUBLE / DOUBLE | осадки, мм |
| `wind_speed_kmh` | DOUBLE / DOUBLE | скорость ветра на высоте 10 м, км/ч |

В принятой части данных все поля заполнены. Температура должна быть от −100 до
70 °C, осадки и скорость ветра — неотрицательными, время — внутри 2021–2025
годов, а `location_id` — присутствовать в справочнике пяти городов.

Parquet разбит по `year/month`, Iceberg — по `months(observed_at)`.
