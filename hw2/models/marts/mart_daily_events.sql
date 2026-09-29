select
    md5(concat_ws(':',
        fact.date_key::text,
        fact.start_hour::text,
        fact.category_key,
        fact.place_key
    )) as mart_key,
    date_dim.event_date,
    fact.start_hour,
    category.category_slug,
    place.place_title,
    count(*)::int as event_count,
    count(*) filter (where fact.is_free)::int as free_event_count,
    round(sum(coalesce(fact.duration_minutes, 0))::numeric, 2)
        as total_duration_minutes
from {{ ref('fct_event_occurrence') }} fact
join {{ ref('dim_date') }} date_dim using (date_key)
join {{ ref('dim_category') }} category using (category_key)
join {{ ref('dim_place') }} place using (place_key)
group by
    fact.date_key,
    date_dim.event_date,
    fact.start_hour,
    fact.category_key,
    category.category_slug,
    fact.place_key,
    place.place_title
