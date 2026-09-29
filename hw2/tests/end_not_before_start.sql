select *
from {{ ref('fct_event_occurrence') }}
where end_at is not null
  and end_at < start_at
