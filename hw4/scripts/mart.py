#!/usr/bin/env python3

import hashlib
import json
from datetime import date, datetime
from decimal import Decimal


MART_QUERY = """
select
    snapshot_date,
    region_code,
    region_name,
    sphere_name,
    currency,
    sum(vacancy_count) as vacancy_count,
    sum(workplace_count) as workplace_count,
    sum(salary_min_sum) as total_salary_min,
    sum(salary_min_count) as total_salary_count,
    if(
        sum(salary_min_count) = 0,
        null,
        round(
            toDecimal128(sum(salary_min_sum), 6) / sum(salary_min_count),
            2
        )
    ) as average_salary_min
from hw4.vacancy_mart
group by snapshot_date, region_code, region_name, sphere_name, currency
order by snapshot_date, region_code, region_name, sphere_name, currency
"""

DIRECT_QUERY = """
select
    snapshot_date,
    region_code,
    region_name,
    sphere_name,
    ifNull(currency, 'Не указано') as currency,
    count() as vacancy_count,
    sum(work_places) as workplace_count,
    sum(ifNull(salary_min, toDecimal64(0, 2))) as salary_min_sum,
    countIf(isNotNull(salary_min)) as salary_min_count,
    if(
        countIf(isNotNull(salary_min)) = 0,
        null,
        round(
            toDecimal128(
                sum(ifNull(salary_min, toDecimal64(0, 2))),
                6
            ) /
                countIf(isNotNull(salary_min)),
            2
        )
    ) as average_salary_min
from hw4.vacancy_events
group by snapshot_date, region_code, region_name, sphere_name, currency
order by snapshot_date, region_code, region_name, sphere_name, currency
"""


def aggregate_rows(rows):
    vacancy_count = 0
    workplace_count = 0
    salary_min_sum = Decimal("0")
    salary_min_count = 0
    for row in rows:
        vacancy_count += 1
        workplace_count += int(row["work_places"])
        if row.get("salary_min") not in (None, ""):
            salary_min_sum += Decimal(str(row["salary_min"]))
            salary_min_count += 1
    average = (
        salary_min_sum / salary_min_count
        if salary_min_count
        else None
    )
    return {
        "vacancy_count": vacancy_count,
        "workplace_count": workplace_count,
        "salary_min_sum": salary_min_sum,
        "salary_min_count": salary_min_count,
        "average_salary_min": average,
    }


def summarize_parts(parts):
    salary_sum = sum(
        (Decimal(str(part["salary_min_sum"])) for part in parts),
        Decimal("0"),
    )
    salary_count = sum(int(part["salary_min_count"]) for part in parts)
    return {
        "salary_min_sum": salary_sum,
        "salary_min_count": salary_count,
        "average_salary_min": salary_sum / salary_count if salary_count else None,
    }


def mart_rows(client):
    return list(client.query(MART_QUERY).result_rows)


def direct_rows(client):
    return list(client.query(DIRECT_QUERY).result_rows)


def _json_value(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    return value


def rows_digest(rows):
    normalized = [tuple(_json_value(value) for value in row) for row in rows]
    payload = json.dumps(
        normalized,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
