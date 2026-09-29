with history as (
    select * from {{ ref('diagnostic_venue_history') }}
)
select
    left_row.source_system,
    left_row.place_id,
    left_row.valid_from as left_valid_from,
    left_row.valid_to as left_valid_to,
    right_row.valid_from as right_valid_from,
    right_row.valid_to as right_valid_to
from history left_row
join history right_row
  on left_row.source_system = right_row.source_system
 and left_row.place_id = right_row.place_id
 and left_row.valid_from < right_row.valid_from
 and left_row.valid_from < coalesce(right_row.valid_to, timestamp '9999-12-31')
 and right_row.valid_from < coalesce(left_row.valid_to, timestamp '9999-12-31')
