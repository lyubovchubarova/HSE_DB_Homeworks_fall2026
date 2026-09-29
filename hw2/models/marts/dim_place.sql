select distinct on (source_system, place_id)
    md5(concat_ws(':', source_system, place_id::text)) as place_key,
    source_system,
    place_id,
    place_title
from {{ ref('stg_kudago_event_occurrences') }}
order by source_system, place_id, loaded_at desc, event_id desc
