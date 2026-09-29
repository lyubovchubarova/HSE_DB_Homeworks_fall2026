with expanded as (
    select
        source_system,
        event_id,
        payload,
        period,
        loaded_at
    from {{ source('raw', 'kudago_events') }}
    cross join lateral jsonb_array_elements(payload -> 'dates') as period
),
typed as (
    select
        md5(concat_ws(':', source_system, event_id::text, period ->> 'start'))
            as event_occurrence_key,
        source_system,
        event_id,
        nullif(trim(payload ->> 'title'), '') as event_title,
        to_timestamp((payload ->> 'publication_date')::bigint) as publication_at,
        to_timestamp((period ->> 'start')::bigint) as start_at,
        case
            when coalesce((period ->> 'end')::bigint, 0) > 0
                then to_timestamp((period ->> 'end')::bigint)
        end as end_at,
        (to_timestamp((period ->> 'start')::bigint) at time zone 'Europe/Moscow')::date
            as event_date,
        extract(hour from to_timestamp((period ->> 'start')::bigint) at time zone 'Europe/Moscow')::int
            as start_hour,
        coalesce(payload -> 'categories' ->> 0, 'unknown') as category_slug,
        coalesce((payload -> 'place' ->> 'id')::bigint, 0) as place_id,
        coalesce(nullif(trim(payload -> 'place' ->> 'title'), ''), 'Не указано')
            as place_title,
        coalesce((payload ->> 'is_free')::boolean, false) as is_free,
        nullif(trim(payload ->> 'age_restriction'), '') as age_restriction,
        payload ->> 'site_url' as site_url,
        loaded_at
    from expanded
)
select distinct on (source_system, event_id, start_at)
    *,
    case
        when end_at is null then null
        else extract(epoch from (end_at - start_at)) / 60.0
    end as duration_minutes
from typed
order by
    source_system,
    event_id,
    start_at,
    loaded_at desc,
    end_at desc nulls last
