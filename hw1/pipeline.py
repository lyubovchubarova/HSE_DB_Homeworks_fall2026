"""Домашняя работа 1: CSV -> Parquet -> Iceberg."""

from __future__ import annotations

import csv
import hashlib
import time
import urllib.request
from pathlib import Path
from urllib.parse import urlencode


SOURCE = "https://archive-api.open-meteo.com/v1/archive"
START_DATE = "2021-01-01"
END_DATE = "2025-12-31"
SPLIT_DATE = "2024-01-01"
FIELDS = ("temperature_2m", "precipitation", "wind_speed_10m")
CITIES = (
    ("Москва", 55.7558, 37.6173),
    ("Санкт-Петербург", 59.9343, 30.3351),
    ("Екатеринбург", 56.8389, 60.6057),
    ("Новосибирск", 55.0084, 82.9357),
    ("Сочи", 43.6028, 39.7342),
)

LOCAL_DIR = Path("/data/hw1")
RAW_FILE = LOCAL_DIR / "source.csv"
PREPARED_FILE = LOCAL_DIR / "open_meteo.csv"
RAW_KEY = "student_01/open_meteo/ingestion_date=2026-10-06/source.csv"
CSV_PATH = "s3a://datalake/student_01/open_meteo/accepted_csv"
REJECTS_PATH = "s3a://datalake/student_01/open_meteo/rejects"
PARQUET_PATH = "s3a://datalake/student_01/open_meteo/parquet"
TABLE = "lakehouse.student_01.open_meteo"

METADATA_HEADER = [
    "location_id",
    "latitude",
    "longitude",
    "elevation",
    "utc_offset_seconds",
    "timezone",
    "timezone_abbreviation",
]
OBSERVATION_HEADER = [
    "location_id",
    "time",
    "temperature_2m (°C)",
    "precipitation (mm)",
    "wind_speed_10m (km/h)",
]
PREPARED_HEADER = [
    "location_id",
    "observed_at_raw",
    "temperature_raw",
    "precipitation_raw",
    "wind_raw",
]
FINAL_COLUMNS = [
    "location_id",
    "city",
    "timezone",
    "observed_at",
    "observation_date",
    "year",
    "month",
    "hour",
    "temperature_c",
    "precipitation_mm",
    "wind_speed_kmh",
]


def source_url() -> str:
    """Собирает ссылку для загрузки данных."""
    params = {
        "latitude": ",".join(str(city[1]) for city in CITIES),
        "longitude": ",".join(str(city[2]) for city in CITIES),
        "start_date": START_DATE,
        "end_date": END_DATE,
        "hourly": ",".join(FIELDS),
        "timezone": "auto",
        "format": "csv",
    }
    return f"{SOURCE}?{urlencode(params)}"


def prepare_csv(raw: bytes) -> tuple[bytes, list[tuple[int, str, str]], int]:
    """Отделяет данные о городах от строк с погодой."""
    lines = raw.decode("utf-8-sig").splitlines()
    if len(lines) < 8:
        raise ValueError("Неожиданный формат Open-Meteo")

    rows = [next(csv.reader([line])) for line in lines]
    if rows[0] != METADATA_HEADER:
        raise ValueError("Неожиданный формат Open-Meteo: нет метаданных")

    try:
        observations_at = rows.index(OBSERVATION_HEADER)
    except ValueError as error:
        raise ValueError("Неожиданный формат Open-Meteo: нет наблюдений") from error

    metadata_rows = [row for row in rows[1:observations_at] if row]
    if len(metadata_rows) != len(CITIES):
        raise ValueError("Неожиданный формат Open-Meteo: неверное число городов")

    cities: list[tuple[int, str, str]] = []
    for expected_id, (row, city) in enumerate(zip(metadata_rows, CITIES, strict=True)):
        if len(row) != len(METADATA_HEADER) or int(row[0]) != expected_id:
            raise ValueError("Неожиданный формат Open-Meteo: неверный location_id")
        if not row[5]:
            raise ValueError("Неожиданный формат Open-Meteo: нет timezone")
        cities.append((expected_id, city[0], row[5]))

    observations = [row for row in rows[observations_at + 1 :] if row]
    if not observations or any(len(row) != len(OBSERVATION_HEADER) for row in observations):
        raise ValueError("Неожиданный формат Open-Meteo: неверные строки наблюдений")

    output = [PREPARED_HEADER, *observations]
    prepared = "\n".join(",".join(row) for row in output).encode() + b"\n"
    return prepared, cities, len(observations)


def iceberg_filters() -> tuple[str, str]:
    """Делит данные на две части по дате."""
    boundary = f"{SPLIT_DATE} 00:00:00"
    return (
        f"observed_at < TIMESTAMP_NTZ '{boundary}'",
        f"observed_at >= TIMESTAMP_NTZ '{boundary}'",
    )


def staging_schema():
    from pyspark.sql.types import StringType, StructField, StructType

    return StructType([StructField(name, StringType(), True) for name in PREPARED_HEADER])


def final_schema():
    from pyspark.sql.types import (
        DateType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampNTZType,
    )

    types = [
        IntegerType(),
        StringType(),
        StringType(),
        TimestampNTZType(),
        DateType(),
        IntegerType(),
        IntegerType(),
        IntegerType(),
        DoubleType(),
        DoubleType(),
        DoubleType(),
    ]
    return StructType(
        [StructField(name, data_type, True) for name, data_type in zip(FINAL_COLUMNS, types)]
    )


def path_size(spark, path: str) -> int:
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    filesystem = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return int(filesystem.getContentSummary(hadoop_path).getLength())


def main() -> None:
    # Скачиваем файл и сохраняем его без изменений.
    url = source_url()
    request = urllib.request.Request(url, headers={"User-Agent": "HSE-DB-HW1"})
    with urllib.request.urlopen(request, timeout=120) as response:
        raw = response.read()
    if not raw:
        raise ValueError("Open-Meteo вернул пустой файл")

    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    RAW_FILE.write_bytes(raw)
    prepared, cities, observation_count = prepare_csv(raw)
    PREPARED_FILE.write_bytes(prepared)

    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="hse2026minio",
    )
    try:
        s3.put_object(Bucket="raw", Key=RAW_KEY, Body=raw)
    finally:
        s3.close()

    print(f"Source URL: {url}")
    print(f"Raw bytes: {len(raw)}")
    print(f"Raw SHA-256: {hashlib.sha256(raw).hexdigest()}")
    print(f"Raw object: s3://raw/{RAW_KEY}")
    print(f"Prepared observation rows: {observation_count}")

    # Читаем CSV и проверяем данные.
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    spark = (
        SparkSession.builder.appName("hw1-open-meteo")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    checked = None
    try:
        source = (
            spark.read.schema(staging_schema())
            .option("header", True)
            .option("mode", "FAILFAST")
            .csv(str(PREPARED_FILE))
        )
        city_schema = StructType(
            [
                StructField("known_location_id", IntegerType(), False),
                StructField("city", StringType(), False),
                StructField("timezone", StringType(), False),
            ]
        )
        city_df = spark.createDataFrame(cities, city_schema)

        checked = (
            source.withColumn(
                "_location_id",
                F.when(F.trim(F.col("location_id")).rlike(r"^[0-9]+$"), F.col("location_id").cast("int")),
            )
            .withColumn(
                "_observed_at",
                F.to_timestamp_ntz(F.col("observed_at_raw"), F.lit("yyyy-MM-dd'T'HH:mm")),
            )
            .withColumn("_temperature", F.col("temperature_raw").cast("double"))
            .withColumn("_precipitation", F.col("precipitation_raw").cast("double"))
            .withColumn("_wind", F.col("wind_raw").cast("double"))
            .join(F.broadcast(city_df), F.col("_location_id") == F.col("known_location_id"), "left")
        )

        start = F.lit(f"{START_DATE} 00:00:00").cast("timestamp_ntz")
        end = F.lit("2026-01-01 00:00:00").cast("timestamp_ntz")
        reason = (
            F.when(F.col("known_location_id").isNull(), "invalid location_id")
            .when(F.col("_observed_at").isNull(), "invalid time")
            .when((F.col("_observed_at") < start) | (F.col("_observed_at") >= end), "time outside range")
            .when(
                F.col("_temperature").isNull()
                | F.isnan("_temperature")
                | (F.col("_temperature") < -100)
                | (F.col("_temperature") > 70),
                "invalid temperature",
            )
            .when(
                F.col("_precipitation").isNull()
                | F.isnan("_precipitation")
                | (F.col("_precipitation") < 0),
                "invalid precipitation",
            )
            .when(
                F.col("_wind").isNull() | F.isnan("_wind") | (F.col("_wind") < 0),
                "invalid wind",
            )
            .otherwise("")
        )
        checked = checked.withColumn("reject_reason", reason).cache()

        accepted = checked.where(F.col("reject_reason") == "").select(
            F.col("_location_id").alias("location_id"),
            "city",
            "timezone",
            F.col("_observed_at").alias("observed_at"),
            F.to_date("_observed_at").alias("observation_date"),
            F.year("_observed_at").alias("year"),
            F.month("_observed_at").alias("month"),
            F.hour("_observed_at").alias("hour"),
            F.col("_temperature").alias("temperature_c"),
            F.col("_precipitation").alias("precipitation_mm"),
            F.col("_wind").alias("wind_speed_kmh"),
        )
        rejected = checked.where(F.col("reject_reason") != "").select(
            *PREPARED_HEADER, "reject_reason"
        )

        source_count = checked.count()
        accepted_count = accepted.count()
        rejected_count = rejected.count()
        null_temperature = checked.where(F.col("_temperature").isNull()).count()
        print(f"Source count: {source_count}")
        print(f"Accepted count: {accepted_count}")
        print(f"Rejected count: {rejected_count}")
        print(f"Null temperature share: {null_temperature / source_count:.6%}")
        if source_count == 0 or source_count != accepted_count + rejected_count:
            raise ValueError("Не сошёлся контроль количества строк")

        rejected.write.mode("overwrite").option("header", True).csv(REJECTS_PATH)
        accepted.write.mode("overwrite").option("header", True).option(
            "timestampNTZFormat", "yyyy-MM-dd'T'HH:mm:ss"
        ).csv(CSV_PATH)
        accepted.write.mode("overwrite").partitionBy("year", "month").parquet(PARQUET_PATH)

        csv_df = (
            spark.read.schema(final_schema())
            .option("header", True)
            .option("timestampNTZFormat", "yyyy-MM-dd'T'HH:mm:ss")
            .csv(CSV_PATH)
        )
        parquet_df = spark.read.parquet(PARQUET_PATH)
        csv_count = csv_df.count()
        parquet_count = parquet_df.count()
        print(f"Read-back counts: csv={csv_count}, parquet={parquet_count}")
        if csv_count != accepted_count or parquet_count != accepted_count:
            raise ValueError("После чтения изменилось количество строк")

        print(f"Accepted CSV bytes: {path_size(spark, CSV_PATH)}")
        print(f"Parquet bytes: {path_size(spark, PARQUET_PATH)}")

        # Три раза запускаем один запрос для CSV и Parquet.
        for name, dataframe in (("CSV", csv_df), ("Parquet", parquet_df)):
            dataframe.createOrReplaceTempView("benchmark_data")
            for trial in range(1, 4):
                started = time.perf_counter()
                row = spark.sql(
                    """
                    SELECT count(*) observations, avg(temperature_c) average_temperature_c
                    FROM benchmark_data
                    WHERE city = 'Москва' AND year = 2023
                    """
                ).first()
                elapsed = time.perf_counter() - started
                print(
                    f"Benchmark {name} {trial}: {elapsed:.6f}s; "
                    f"observations={row['observations']}; "
                    f"avg_temperature_c={row['average_temperature_c']:.6f}"
                )

        # Записываем две части данных отдельно.
        before, after = iceberg_filters()
        parquet_df.createOrReplaceTempView("parquet_source")
        first_count = parquet_df.where(F.expr(before)).count()
        second_count = parquet_df.where(F.expr(after)).count()
        print(f"Iceberg batch counts: first={first_count}, second={second_count}")
        if first_count + second_count != parquet_count or min(first_count, second_count) == 0:
            raise ValueError("Диапазоны Iceberg пересекаются или не покрывают данные")

        spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.student_01")
        columns = ", ".join(FINAL_COLUMNS)
        spark.sql(
            f"""
            CREATE OR REPLACE TABLE {TABLE}
            USING iceberg
            PARTITIONED BY (months(observed_at))
            AS SELECT {columns} FROM parquet_source WHERE {before}
            """
        )
        print(f"Iceberg count after first write: {spark.table(TABLE).count()}")
        spark.sql(
            f"INSERT INTO {TABLE} SELECT {columns} FROM parquet_source WHERE {after}"
        )
        iceberg_count = spark.table(TABLE).count()
        print(f"Iceberg final count: {iceberg_count}")
        if iceberg_count != parquet_count:
            raise ValueError("Количество строк в Iceberg не совпало с Parquet")

        print("Iceberg snapshots:")
        spark.sql(
            f"""
            SELECT snapshot_id, committed_at, operation,
                   summary['total-records'] AS total_records
            FROM {TABLE}.snapshots ORDER BY committed_at
            """
        ).show(truncate=False)

        control = spark.sql(
            f"""
            SELECT count(*) observations,
                   CAST(avg(temperature_c) AS DECIMAL(18,6)) average_temperature_c
            FROM {TABLE}
            WHERE city = 'Москва'
              AND observed_at >= TIMESTAMP_NTZ '2023-01-01 00:00:00'
              AND observed_at < TIMESTAMP_NTZ '2024-01-01 00:00:00'
            """
        ).first()
        print(
            "SPARK RESULT: "
            f"observations={control['observations']}; "
            f"average_temperature_c={control['average_temperature_c']}"
        )
    finally:
        if checked is not None:
            checked.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
