select distinct
    to_char(event_date, 'YYYYMMDD')::int as date_key,
    event_date,
    extract(isodow from event_date)::int as iso_day_of_week,
    trim(to_char(event_date, 'TMDay')) as day_name,
    extract(week from event_date)::int as iso_week
from {{ ref('stg_kudago_event_occurrences') }}
