select distinct
    md5(category_slug) as category_key,
    category_slug
from {{ ref('stg_kudago_event_occurrences') }}
