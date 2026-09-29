select
    event_occurrence_key,
    source_system,
    event_id,
    event_title,
    to_char(event_date, 'YYYYMMDD')::int as date_key,
    md5(category_slug) as category_key,
    md5(concat_ws(':', source_system, place_id::text)) as place_key,
    start_at,
    end_at,
    start_hour,
    duration_minutes,
    is_free,
    age_restriction,
    site_url
from {{ ref('stg_kudago_event_occurrences') }}
