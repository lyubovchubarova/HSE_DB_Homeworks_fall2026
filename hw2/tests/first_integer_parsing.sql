with examples(value, expected) as (
    values
        ('от 1 до 3', 1),
        ('1.5', 1),
        ('не требуется', null::integer),
        (null::text, null::integer)
), actual as (
    select
        value,
        expected,
        {{ first_integer('value') }} as actual
    from examples
)
select *
from actual
where actual is distinct from expected
