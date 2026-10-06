"""Reproducible Open-Meteo ingestion, DQ, storage, and benchmark pipeline."""

from __future__ import annotations

import csv
import hashlib
import math
import time
import urllib.request
from pathlib import Path
from urllib.parse import urlencode


SOURCE_ENDPOINT = "https://archive-api.open-meteo.com/v1/archive"
CITIES = (
    ("Москва", 55.7558, 37.6173),
    ("Санкт-Петербург", 59.9343, 30.3351),
    ("Екатеринбург", 56.8389, 60.6057),
    ("Новосибирск", 55.0084, 82.9357),
    ("Сочи", 43.6028, 39.7342),
)
START_DATE = "2021-01-01"
END_DATE = "2025-12-31"
HOURLY_FIELDS = ("temperature_2m", "precipitation", "wind_speed_10m")

METADATA_HEADER = (
    "location_id",
    "latitude",
    "longitude",
    "elevation",
    "utc_offset_seconds",
    "timezone",
    "timezone_abbreviation",
)
OBSERVATION_HEADER = (
    "location_id",
    "time",
    "temperature_2m (°C)",
    "precipitation (mm)",
    "wind_speed_10m (km/h)",
)
NORMALIZED_HEADER = (
    "location_id",
    "observed_at_raw",
    "temperature_c_raw",
    "precipitation_mm_raw",
    "wind_speed_kmh_raw",
)

LOCAL_DIR = Path("/data/hw1")
LOCAL_RAW_PATH = LOCAL_DIR / "source.csv"
LOCAL_NORMALIZED_PATH = LOCAL_DIR / "open_meteo_normalized.csv"
RAW_BUCKET = "raw"
RAW_KEY = "student_01/open_meteo/ingestion_date=2026-10-06/source.csv"
ACCEPTED_CSV_PATH = "s3a://datalake/student_01/open_meteo/accepted_csv"
REJECTS_PATH = "s3a://datalake/student_01/open_meteo/rejects"
PARQUET_PATH = "s3a://datalake/student_01/open_meteo/parquet"
TABLE = "lakehouse.student_01.open_meteo"
BATCH_BOUNDARY = "2024-01-01"
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "hse2026minio"
DOWNLOAD_TIMEOUT_SECONDS = 120
USER_AGENT = "HSE-DB-HW1-Open-Meteo/1.0"
OBSERVED_AT_PATTERN = "yyyy-MM-dd'T'HH:mm"
TIMESTAMP_NTZ_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"
DATE_FORMAT = "yyyy-MM-dd"
FINAL_COLUMNS = (
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
)


def benchmark_format_order(trial: int) -> tuple[str, str]:
    """Alternate which storage format is measured first in each trial."""
    if trial % 2:
        return ("CSV", "Parquet")
    return ("Parquet", "CSV")


def batch_predicates() -> tuple[str, str]:
    """Return complementary timestamp_ntz predicates for the two writes."""
    boundary = f"{BATCH_BOUNDARY} 00:00:00"
    return (
        f"observed_at < TIMESTAMP_NTZ '{boundary}'",
        f"observed_at >= TIMESTAMP_NTZ '{boundary}'",
    )


def analytical_aggregation_sql(source: str) -> str:
    """Build the deterministic city/month/hour analytical aggregation."""
    return (
        "SELECT city, month, hour,\n"
        "       count(*) AS observations,\n"
        "       avg(temperature_c) AS average_temperature_c\n"
        f"FROM {source}\n"
        "GROUP BY city, month, hour\n"
        "ORDER BY city, month, hour"
    )


def build_source_url() -> str:
    """Build the deterministic Open-Meteo archive request URL."""
    query = urlencode(
        {
            "latitude": ",".join(str(city[1]) for city in CITIES),
            "longitude": ",".join(str(city[2]) for city in CITIES),
            "start_date": START_DATE,
            "end_date": END_DATE,
            "hourly": ",".join(HOURLY_FIELDS),
            "timezone": "auto",
            "format": "csv",
        }
    )
    return f"{SOURCE_ENDPOINT}?{query}"


def build_city_dimensions(
    metadata_rows: list[dict[str, str]],
) -> list[dict[str, str | int]]:
    """Build validated city/timezone dimension rows in source order."""
    if len(metadata_rows) != len(CITIES):
        raise ValueError(
            f"Expected exactly {len(CITIES)} metadata rows, got {len(metadata_rows)}"
        )

    dimensions: list[dict[str, str | int]] = []
    for expected_location_id, (metadata, city_data) in enumerate(
        zip(metadata_rows, CITIES, strict=True)
    ):
        try:
            location_id = int(metadata["location_id"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("Invalid metadata location_id") from error
        if location_id != expected_location_id:
            raise ValueError(
                "Invalid metadata location_id sequence: "
                f"expected {expected_location_id}, got {location_id}"
            )

        timezone = metadata.get("timezone", "").strip()
        if not timezone:
            raise ValueError(f"Missing timezone for location_id {location_id}")

        dimensions.append(
            {
                "location_id": location_id,
                "city": city_data[0],
                "timezone": timezone,
            }
        )
    return dimensions


def _parse_row(line: str, section: str) -> list[str]:
    try:
        return next(csv.reader([line], strict=True))
    except csv.Error as error:
        raise ValueError(f"Invalid Open-Meteo CSV {section}: {error}") from error


def normalize_open_meteo_csv(
    raw_bytes: bytes,
) -> tuple[bytes, list[dict[str, str]], int]:
    """Validate Open-Meteo sections and return a Spark-friendly CSV."""
    text = raw_bytes.decode("utf-8", errors="strict")
    lines = text.splitlines()

    if not lines or tuple(_parse_row(lines[0], "metadata header")) != METADATA_HEADER:
        raise ValueError("Invalid Open-Meteo CSV metadata header")

    metadata_lines = lines[1 : 1 + len(CITIES)]
    if len(metadata_lines) != len(CITIES):
        raise ValueError("Invalid Open-Meteo CSV: expected exactly 5 metadata rows")

    metadata_rows: list[dict[str, str]] = []
    for expected_location_id, line in enumerate(metadata_lines):
        values = _parse_row(line, "metadata row")
        if len(values) != len(METADATA_HEADER):
            raise ValueError(
                "Invalid Open-Meteo CSV metadata columns: "
                f"expected {len(METADATA_HEADER)}, got {len(values)}"
            )
        if values[0] != str(expected_location_id):
            raise ValueError(
                "Invalid Open-Meteo CSV metadata location_id sequence: "
                f"expected {expected_location_id}, got {values[0]!r}"
            )
        metadata_rows.append(dict(zip(METADATA_HEADER, values, strict=True)))

    header_index = 1 + len(CITIES)
    while header_index < len(lines) and not lines[header_index]:
        header_index += 1
    if (
        header_index >= len(lines)
        or tuple(_parse_row(lines[header_index], "observation header"))
        != OBSERVATION_HEADER
    ):
        raise ValueError("Invalid Open-Meteo CSV observation header")

    observation_lines = lines[header_index + 1 :]
    if not observation_lines:
        raise ValueError("Invalid Open-Meteo CSV: observations are empty")

    for line in observation_lines:
        values = _parse_row(line, "observation row")
        if len(values) != len(OBSERVATION_HEADER):
            raise ValueError(
                "Invalid Open-Meteo CSV observation columns: "
                f"expected {len(OBSERVATION_HEADER)}, got {len(values)}"
            )

    normalized_text = "\n".join(
        [",".join(NORMALIZED_HEADER), *observation_lines]
    ) + "\n"
    return normalized_text.encode("utf-8"), metadata_rows, len(observation_lines)


def download_source() -> bytes:
    """Download and persist the byte-identical raw Open-Meteo response."""
    url = build_source_url()
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(
        request, timeout=DOWNLOAD_TIMEOUT_SECONDS
    ) as response:
        raw_bytes = response.read()
    if not raw_bytes:
        raise ValueError("Open-Meteo returned an empty response")

    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    LOCAL_RAW_PATH.write_bytes(raw_bytes)

    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    try:
        s3.put_object(Bucket=RAW_BUCKET, Key=RAW_KEY, Body=raw_bytes)
    finally:
        s3.close()

    digest = hashlib.sha256(raw_bytes).hexdigest()
    print(f"Source URL: {url}")
    print(f"Raw byte size: {len(raw_bytes)}")
    print(f"Raw SHA-256: {digest}")
    print(f"s3://{RAW_BUCKET}/{RAW_KEY}")
    return raw_bytes


def create_spark_session():
    """Create the Spark engine used for DQ, storage, and benchmarking."""
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("hw1-open-meteo")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    return spark


def _staging_schema():
    from pyspark.sql.types import StringType, StructField, StructType

    return StructType(
        [StructField(column, StringType(), True) for column in NORMALIZED_HEADER]
    )


def _final_schema():
    from pyspark.sql.types import (
        DateType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampNTZType,
    )

    return StructType(
        [
            StructField("location_id", IntegerType(), True),
            StructField("city", StringType(), True),
            StructField("timezone", StringType(), True),
            StructField("observed_at", TimestampNTZType(), True),
            StructField("observation_date", DateType(), True),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("hour", IntegerType(), True),
            StructField("temperature_c", DoubleType(), True),
            StructField("precipitation_mm", DoubleType(), True),
            StructField("wind_speed_kmh", DoubleType(), True),
        ]
    )


def _city_schema():
    from pyspark.sql.types import (
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("dimension_location_id", IntegerType(), False),
            StructField("city", StringType(), False),
            StructField("timezone", StringType(), False),
        ]
    )


def _missing(column):
    from pyspark.sql import functions as F

    return column.isNull() | (F.trim(column) == "")


def _nonfinite(column):
    from pyspark.sql import functions as F

    return F.isnan(column) | (F.abs(column) == F.lit(float("inf")))


def _verify_spark_runtime(spark) -> None:
    """Fail fast unless Spark can execute required 3.5-only expressions."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import TimestampNTZType

    probe = spark.range(1).select(
        F.to_timestamp_ntz(
            F.lit("2023-01-02T03:04"),
            F.lit(OBSERVED_AT_PATTERN),
        ).alias("observed_at"),
        _nonfinite(F.lit(float("nan"))).alias("nan_is_nonfinite"),
        _nonfinite(F.lit(float("inf"))).alias("inf_is_nonfinite"),
        _nonfinite(F.lit(float("-inf"))).alias("negative_inf_is_nonfinite"),
        _nonfinite(F.lit(1.0)).alias("finite_is_nonfinite"),
    )
    if not isinstance(probe.schema["observed_at"].dataType, TimestampNTZType):
        raise RuntimeError("Spark did not produce timestamp_ntz for observed_at")

    result = probe.first()
    if result["observed_at"].isoformat(timespec="minutes") != "2023-01-02T03:04":
        raise RuntimeError("Spark timestamp_ntz wall-clock self-check failed")
    finite_flags = tuple(
        result[name]
        for name in (
            "nan_is_nonfinite",
            "inf_is_nonfinite",
            "negative_inf_is_nonfinite",
            "finite_is_nonfinite",
        )
    )
    if finite_flags != (
        True,
        True,
        True,
        False,
    ):
        raise RuntimeError("Spark non-finite numeric self-check failed")
    print("Spark runtime self-check: timestamp_ntz and non-finite DQ supported")


def _reject_reason_expression():
    from pyspark.sql import functions as F

    location_raw = F.col("location_id")
    time_raw = F.col("observed_at_raw")
    temperature_raw = F.col("temperature_c_raw")
    precipitation_raw = F.col("precipitation_mm_raw")
    wind_raw = F.col("wind_speed_kmh_raw")

    location_missing = _missing(location_raw)
    time_missing = _missing(time_raw)
    temperature_missing = _missing(temperature_raw)
    precipitation_missing = _missing(precipitation_raw)
    wind_missing = _missing(wind_raw)

    reasons = [
        F.when(location_missing, F.lit("missing location_id")),
        F.when(
            ~location_missing & F.col("_location_id").isNull(),
            F.lit("noninteger location_id"),
        ),
        F.when(
            F.col("_location_id").isNotNull()
            & F.col("dimension_location_id").isNull(),
            F.lit("unknown location_id"),
        ),
        F.when(time_missing, F.lit("missing time")),
        F.when(
            ~time_missing & F.col("_observed_at").isNull(),
            F.lit("unparseable time"),
        ),
        F.when(temperature_missing, F.lit("missing temperature")),
        F.when(
            ~temperature_missing & F.col("_temperature_c").isNull(),
            F.lit("nonnumeric temperature"),
        ),
        F.when(
            F.col("_temperature_c").isNotNull()
            & _nonfinite(F.col("_temperature_c")),
            F.lit("nonfinite temperature"),
        ),
        F.when(
            F.col("_temperature_c").isNotNull()
            & ~_nonfinite(F.col("_temperature_c"))
            & (
                (F.col("_temperature_c") < F.lit(-100.0))
                | (F.col("_temperature_c") > F.lit(70.0))
            ),
            F.lit("temperature outside [-100,70]"),
        ),
        F.when(precipitation_missing, F.lit("missing precipitation")),
        F.when(
            ~precipitation_missing & F.col("_precipitation_mm").isNull(),
            F.lit("nonnumeric precipitation"),
        ),
        F.when(
            F.col("_precipitation_mm").isNotNull()
            & _nonfinite(F.col("_precipitation_mm")),
            F.lit("nonfinite precipitation"),
        ),
        F.when(
            F.col("_precipitation_mm").isNotNull()
            & ~_nonfinite(F.col("_precipitation_mm"))
            & (F.col("_precipitation_mm") < F.lit(0.0)),
            F.lit("negative precipitation"),
        ),
        F.when(wind_missing, F.lit("missing wind")),
        F.when(
            ~wind_missing & F.col("_wind_speed_kmh").isNull(),
            F.lit("nonnumeric wind"),
        ),
        F.when(
            F.col("_wind_speed_kmh").isNotNull()
            & _nonfinite(F.col("_wind_speed_kmh")),
            F.lit("nonfinite wind"),
        ),
        F.when(
            F.col("_wind_speed_kmh").isNotNull()
            & ~_nonfinite(F.col("_wind_speed_kmh"))
            & (F.col("_wind_speed_kmh") < F.lit(0.0)),
            F.lit("negative wind"),
        ),
    ]
    return F.concat_ws("; ", *reasons)


def _read_staging(spark):
    return (
        spark.read.schema(_staging_schema())
        .option("header", True)
        .option("enforceSchema", True)
        .option("mode", "FAILFAST")
        .csv(str(LOCAL_NORMALIZED_PATH))
    )


def _classify_rows(spark, city_dimensions):
    from pyspark.sql import functions as F

    staging = _read_staging(spark)
    city_rows = [
        (row["location_id"], row["city"], row["timezone"])
        for row in city_dimensions
    ]
    cities = spark.createDataFrame(city_rows, schema=_city_schema())

    location_is_integer = F.trim(F.col("location_id")).rlike(r"^[+-]?\d+$")
    typed = (
        staging.withColumn(
            "_location_id",
            F.when(
                location_is_integer,
                F.trim(F.col("location_id")).cast("int"),
            ),
        )
        .withColumn(
            "_observed_at",
            F.to_timestamp_ntz(
                F.col("observed_at_raw"),
                F.lit(OBSERVED_AT_PATTERN),
            ),
        )
        .withColumn(
            "_temperature_c",
            F.trim(F.col("temperature_c_raw")).cast("double"),
        )
        .withColumn(
            "_precipitation_mm",
            F.trim(F.col("precipitation_mm_raw")).cast("double"),
        )
        .withColumn(
            "_wind_speed_kmh",
            F.trim(F.col("wind_speed_kmh_raw")).cast("double"),
        )
    )
    joined = typed.join(
        F.broadcast(cities),
        typed["_location_id"] == cities["dimension_location_id"],
        "left",
    )
    return joined.withColumn("reject_reason", _reject_reason_expression())


def _accepted_rows(classified):
    from pyspark.sql import functions as F

    # Open-Meteo timestamps have no offset and use timestamp_ntz explicitly.
    # The IANA timezone column carries their interpretation; UTC remains only
    # an engine-stability setting and cannot shift these wall-clock values.
    return classified.where(F.col("reject_reason") == "").select(
        F.col("_location_id").cast("int").alias("location_id"),
        F.col("city").cast("string").alias("city"),
        F.col("timezone").cast("string").alias("timezone"),
        F.col("_observed_at").cast("timestamp_ntz").alias("observed_at"),
        F.to_date(F.col("_observed_at")).alias("observation_date"),
        F.year(F.col("_observed_at")).cast("int").alias("year"),
        F.month(F.col("_observed_at")).cast("int").alias("month"),
        F.hour(F.col("_observed_at")).cast("int").alias("hour"),
        F.col("_temperature_c").cast("double").alias("temperature_c"),
        F.col("_precipitation_mm").cast("double").alias("precipitation_mm"),
        F.col("_wind_speed_kmh").cast("double").alias("wind_speed_kmh"),
    )


def _rejected_rows(classified):
    from pyspark.sql import functions as F

    return classified.where(F.col("reject_reason") != "").select(
        *[F.col(column) for column in NORMALIZED_HEADER],
        F.col("reject_reason"),
    )


def _schema_types(schema) -> dict[str, str]:
    return {field.name: field.dataType.simpleString() for field in schema.fields}


def _verify_readbacks(accepted, accepted_csv, parquet) -> int:
    expected_types = _schema_types(_final_schema())
    for name, dataframe in (("accepted CSV", accepted_csv), ("Parquet", parquet)):
        actual_types = _schema_types(dataframe.schema)
        if actual_types != expected_types:
            raise ValueError(
                f"{name} schema mismatch: expected {expected_types}, got {actual_types}"
            )

    expected_count = accepted.count()
    csv_count = accepted_csv.count()
    parquet_count = parquet.count()
    if csv_count != expected_count or parquet_count != expected_count:
        raise ValueError(
            "Read-back count mismatch: "
            f"accepted={expected_count}, csv={csv_count}, parquet={parquet_count}"
        )
    print(
        "Read-back counts: "
        f"accepted={expected_count}, csv={csv_count}, parquet={parquet_count}"
    )
    return expected_count


def _hadoop_path_bytes(spark, path: str) -> int:
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    filesystem = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return int(filesystem.getContentSummary(hadoop_path).getLength())


def _benchmark(spark, accepted_csv, parquet) -> None:
    accepted_csv.createOrReplaceTempView("accepted_csv_benchmark")
    parquet.createOrReplaceTempView("parquet_benchmark")
    queries = {
        "CSV": spark.sql(
            """
            SELECT count(*) AS observations,
                   avg(temperature_c) AS average_temperature_c
            FROM accepted_csv_benchmark
            WHERE city = 'Москва' AND year = 2023
            """
        ),
        "Parquet": spark.sql(
            """
            SELECT count(*) AS observations,
                   avg(temperature_c) AS average_temperature_c
            FROM parquet_benchmark
            WHERE city = 'Москва' AND year = 2023
            """
        ),
    }

    expected_result: tuple[int, float] | None = None
    for trial in range(1, 4):
        for format_name in benchmark_format_order(trial):
            started = time.perf_counter()
            row = queries[format_name].collect()[0]
            elapsed = time.perf_counter() - started
            result = (int(row["observations"]), float(row["average_temperature_c"]))
            if result[0] <= 0:
                raise ValueError("Benchmark query returned no Moscow 2023 rows")
            if expected_result is None:
                expected_result = result
            elif result[0] != expected_result[0] or not math.isclose(
                result[1],
                expected_result[1],
                rel_tol=1e-9,
                abs_tol=1e-9,
            ):
                raise ValueError(
                    f"Benchmark result mismatch: {format_name} returned {result}, "
                    f"expected {expected_result}"
                )
            print(
                f"Benchmark trial {trial} {format_name}: {elapsed:.6f}s; "
                f"observations={result[0]}; avg_temperature_c={result[1]:.6f}"
            )
    print(
        "Benchmark caveat: the first timing is not guaranteed to be cold, and "
        "these timings are workload- and environment-specific, not universal."
    )


def _batch_metrics(dataframe) -> dict[str, int]:
    from pyspark.sql import functions as F

    first_predicate, second_predicate = batch_predicates()
    row = dataframe.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(F.expr(first_predicate), 1).otherwise(0)).alias("first"),
        F.sum(F.when(F.expr(second_predicate), 1).otherwise(0)).alias("second"),
        F.sum(F.when(F.col("observed_at").isNull(), 1).otherwise(0)).alias(
            "null_observed_at"
        ),
        F.sum(
            F.when(
                (F.col("observed_at") < F.lit("2021-01-01").cast("timestamp_ntz"))
                | (
                    F.col("observed_at")
                    >= F.lit("2026-01-01").cast("timestamp_ntz")
                ),
                1,
            ).otherwise(0)
        ).alias("outside_expected_range"),
    ).first()
    return {name: int(row[name] or 0) for name in row.asDict()}


def _validate_parquet_metrics(metrics: dict[str, int]) -> None:
    if metrics["total"] <= 0:
        raise ValueError("Parquet contains no rows")
    if metrics["first"] <= 0 or metrics["second"] <= 0:
        raise ValueError(f"Both Iceberg batches must be non-empty: {metrics}")
    if metrics["first"] + metrics["second"] != metrics["total"]:
        raise ValueError(f"Iceberg batch split does not conserve rows: {metrics}")
    if metrics["null_observed_at"]:
        raise ValueError(
            "Parquet contains null observed_at values: "
            f"{metrics['null_observed_at']}"
        )
    if metrics["outside_expected_range"]:
        raise ValueError(
            "Parquet contains observations outside "
            "[2021-01-01, 2026-01-01): "
            f"{metrics['outside_expected_range']}"
        )


def _snapshot_ids(spark) -> set[int]:
    if not spark.catalog.tableExists(TABLE):
        return set()
    return {
        int(row["snapshot_id"])
        for row in spark.sql(f"SELECT snapshot_id FROM {TABLE}.snapshots").collect()
    }


def _verify_iceberg_schema(spark, parquet) -> None:
    expected_types = _schema_types(parquet.schema)
    actual_types = _schema_types(spark.table(TABLE).schema)
    if actual_types != expected_types:
        raise ValueError(
            "Iceberg schema mismatch: "
            f"expected names/types {expected_types}, got {actual_types}"
        )


def _spark_control(spark, parquet) -> None:
    parquet.createOrReplaceTempView("parquet_iceberg_control")
    query = """
        SELECT count(*) AS observations,
               avg(temperature_c) AS average_temperature_c
        FROM {source}
        WHERE city = 'Москва'
          AND observed_at >= TIMESTAMP_NTZ '2023-01-01 00:00:00'
          AND observed_at < TIMESTAMP_NTZ '2024-01-01 00:00:00'
    """
    iceberg_row = spark.sql(query.format(source=TABLE)).first()
    parquet_row = spark.sql(query.format(source="parquet_iceberg_control")).first()
    iceberg_result = (
        int(iceberg_row["observations"]),
        float(iceberg_row["average_temperature_c"]),
    )
    parquet_result = (
        int(parquet_row["observations"]),
        float(parquet_row["average_temperature_c"]),
    )
    if iceberg_result[0] <= 0:
        raise ValueError("Spark control returned no Moscow local-year 2023 rows")
    if iceberg_result[0] != parquet_result[0] or not math.isclose(
        iceberg_result[1],
        parquet_result[1],
        rel_tol=1e-9,
        abs_tol=1e-9,
    ):
        raise ValueError(
            "Spark control mismatch: "
            f"Iceberg={iceberg_result}, Parquet={parquet_result}"
        )
    print(
        "SPARK CONTROL Iceberg Moscow local-year 2023: "
        f"count={iceberg_result[0]}; avg_temperature_c={iceberg_result[1]:.6f}"
    )
    print(
        "SPARK CONTROL Parquet Moscow local-year 2023: "
        f"count={parquet_result[0]}; avg_temperature_c={parquet_result[1]:.6f}"
    )

    iceberg_aggregates = spark.sql(analytical_aggregation_sql(TABLE)).collect()
    parquet_aggregates = spark.sql(
        analytical_aggregation_sql("parquet_iceberg_control")
    ).collect()
    if len(iceberg_aggregates) != len(parquet_aggregates):
        raise ValueError(
            "Spark analytical control group-count mismatch: "
            f"Iceberg={len(iceberg_aggregates)}, "
            f"Parquet={len(parquet_aggregates)}"
        )

    for iceberg_group, parquet_group in zip(
        iceberg_aggregates, parquet_aggregates, strict=True
    ):
        iceberg_key = (
            iceberg_group["city"],
            int(iceberg_group["month"]),
            int(iceberg_group["hour"]),
        )
        parquet_key = (
            parquet_group["city"],
            int(parquet_group["month"]),
            int(parquet_group["hour"]),
        )
        iceberg_count = int(iceberg_group["observations"])
        parquet_count = int(parquet_group["observations"])
        iceberg_average = float(iceberg_group["average_temperature_c"])
        parquet_average = float(parquet_group["average_temperature_c"])
        if (
            iceberg_key != parquet_key
            or iceberg_count != parquet_count
            or not math.isclose(
                iceberg_average,
                parquet_average,
                rel_tol=1e-9,
                abs_tol=1e-9,
            )
        ):
            raise ValueError(
                "Spark analytical control mismatch: "
                f"Iceberg={(iceberg_key, iceberg_count, iceberg_average)}, "
                f"Parquet={(parquet_key, parquet_count, parquet_average)}"
            )

    print(
        "SPARK ANALYTICAL CONTROL matched all city/month/hour groups: "
        f"{len(iceberg_aggregates)} rows"
    )
    print("SPARK ANALYTICAL CONTROL first 20 ordered Iceberg rows:")
    for row in iceberg_aggregates[:20]:
        print(
            f"city={row['city']}; month={int(row['month'])}; "
            f"hour={int(row['hour'])}; observations={int(row['observations'])}; "
            "avg_temperature_c="
            f"{float(row['average_temperature_c']):.6f}"
        )


def _load_iceberg(spark, parquet) -> None:
    first_predicate, second_predicate = batch_predicates()
    metrics = _batch_metrics(parquet)
    _validate_parquet_metrics(metrics)
    print(
        "Iceberg source metrics: "
        f"total={metrics['total']}, first={metrics['first']}, "
        f"second={metrics['second']}, null_observed_at="
        f"{metrics['null_observed_at']}, outside_expected_range="
        f"{metrics['outside_expected_range']}"
    )
    print(f"Iceberg first split condition: {first_predicate}")
    print(f"Iceberg second split condition: {second_predicate}")

    spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.student_01")
    previous_snapshot_ids = _snapshot_ids(spark)
    print(f"Iceberg previous snapshot IDs: {sorted(previous_snapshot_ids)}")

    columns_sql = ", ".join(FINAL_COLUMNS)
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {TABLE}
        USING iceberg
        PARTITIONED BY (months(observed_at))
        AS
        SELECT {columns_sql}
        FROM parquet.`{PARQUET_PATH}`
        WHERE {first_predicate}
        """
    )
    _verify_iceberg_schema(spark, parquet)
    first_count = spark.table(TABLE).count()
    if first_count != metrics["first"]:
        raise ValueError(
            "Iceberg first-write count mismatch: "
            f"expected {metrics['first']}, got {first_count}"
        )
    first_snapshot_ids = _snapshot_ids(spark)
    create_snapshot_ids = first_snapshot_ids - previous_snapshot_ids
    if not create_snapshot_ids:
        raise ValueError("Iceberg CREATE OR REPLACE produced no new snapshot")
    print(
        "Iceberg snapshot IDs after CREATE OR REPLACE: "
        f"{sorted(first_snapshot_ids)}; new={sorted(create_snapshot_ids)}"
    )

    spark.sql(
        f"""
        INSERT INTO {TABLE}
        SELECT {columns_sql}
        FROM parquet.`{PARQUET_PATH}`
        WHERE {second_predicate}
        """
    )
    final_metrics = _batch_metrics(spark.table(TABLE))
    if final_metrics != metrics:
        raise ValueError(
            "Iceberg final counts do not match Parquet batches: "
            f"expected={metrics}, actual={final_metrics}"
        )
    print(
        "Iceberg final metrics: "
        f"total={final_metrics['total']}, first={final_metrics['first']}, "
        f"second={final_metrics['second']}"
    )
    final_snapshot_ids = _snapshot_ids(spark)
    insert_snapshot_ids = final_snapshot_ids - first_snapshot_ids
    if not insert_snapshot_ids:
        raise ValueError("Iceberg INSERT produced no new snapshot")
    print(
        "Iceberg snapshot IDs after INSERT: "
        f"{sorted(final_snapshot_ids)}; new={sorted(insert_snapshot_ids)}"
    )

    print("Iceberg snapshot history:")
    spark.sql(
        f"""
        SELECT snapshot_id,
               committed_at,
               operation,
               summary['total-records'] AS total_records
        FROM {TABLE}.snapshots
        ORDER BY committed_at, snapshot_id
        """
    ).show(truncate=False)
    _spark_control(spark, parquet)


def run_spark_pipeline(spark, city_dimensions) -> None:
    from pyspark.sql import functions as F

    _verify_spark_runtime(spark)
    classified = _classify_rows(spark, city_dimensions).cache()
    try:
        accepted = _accepted_rows(classified)
        rejected = _rejected_rows(classified)

        source_count = classified.count()
        null_temperature_count = classified.where(
            F.col("_temperature_c").isNull()
        ).count()
        accepted_count = accepted.count()
        rejected_count = rejected.count()
        null_temperature_share = (
            null_temperature_count / source_count if source_count else 0.0
        )
        print(f"Source count: {source_count}")
        print(f"Accepted count: {accepted_count}")
        print(f"Rejected count: {rejected_count}")
        print(
            "Null temperature share before invalid filtering: "
            f"{null_temperature_share:.6%}"
        )

        (
            rejected.write.mode("overwrite")
            .option("header", True)
            .option("dateFormat", DATE_FORMAT)
            .csv(REJECTS_PATH)
        )
        if source_count <= 0:
            raise ValueError("Normalized source contains no rows")
        if source_count != accepted_count + rejected_count:
            raise ValueError(
                "DQ row conservation failed: "
                f"{source_count} != {accepted_count} + {rejected_count}"
            )
        if accepted_count <= 0:
            raise ValueError("DQ rejected every source row")

        (
            accepted.write.mode("overwrite")
            .option("header", True)
            .option("timestampNTZFormat", TIMESTAMP_NTZ_FORMAT)
            .option("dateFormat", DATE_FORMAT)
            .csv(ACCEPTED_CSV_PATH)
        )
        (
            accepted.write.mode("overwrite")
            .option("compression", "snappy")
            .partitionBy("year", "month")
            .parquet(PARQUET_PATH)
        )

        accepted_csv = (
            spark.read.schema(_final_schema())
            .option("header", True)
            .option("enforceSchema", True)
            .option("mode", "FAILFAST")
            .option("timestampNTZFormat", TIMESTAMP_NTZ_FORMAT)
            .option("dateFormat", DATE_FORMAT)
            .csv(ACCEPTED_CSV_PATH)
        )
        parquet = spark.read.parquet(PARQUET_PATH)
        _verify_readbacks(accepted, accepted_csv, parquet)

        raw_uri = LOCAL_RAW_PATH.resolve().as_uri()
        print(f"Raw bytes: {_hadoop_path_bytes(spark, raw_uri)}")
        print(
            "Accepted CSV directory bytes: "
            f"{_hadoop_path_bytes(spark, ACCEPTED_CSV_PATH)}"
        )
        print(
            f"Parquet directory bytes: {_hadoop_path_bytes(spark, PARQUET_PATH)}"
        )
        _benchmark(spark, accepted_csv, parquet)
        _load_iceberg(spark, parquet)
    finally:
        classified.unpersist()


def main() -> None:
    raw_bytes = download_source()
    normalized_bytes, metadata_rows, observation_count = (
        normalize_open_meteo_csv(raw_bytes)
    )
    LOCAL_NORMALIZED_PATH.write_bytes(normalized_bytes)
    print(f"Normalized observation rows: {observation_count}")
    print(f"Normalized CSV: {LOCAL_NORMALIZED_PATH}")
    city_dimensions = build_city_dimensions(metadata_rows)

    spark = None
    try:
        spark = create_spark_session()
        run_spark_pipeline(spark, city_dimensions)
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
