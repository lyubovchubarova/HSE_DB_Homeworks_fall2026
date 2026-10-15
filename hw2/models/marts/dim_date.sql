select distinct
    to_char(snapshot_date, 'YYYYMMDD')::integer as date_key,
    snapshot_date as calendar_date,
    extract(isodow from snapshot_date)::integer as iso_day_of_week,
    extract(month from snapshot_date)::integer as month_number,
    extract(year from snapshot_date)::integer as year_number
from {{ ref('stg_trudvsem_vacancies') }}
