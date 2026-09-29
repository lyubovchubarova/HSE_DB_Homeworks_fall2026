-- 1. В какие дни и по каким категориям больше всего событий?
select
    event_date,
    category_slug,
    sum(event_count) as events
from analytics_public.mart_daily_events
group by event_date, category_slug
order by events desc, event_date, category_slug;

-- 2. Какие площадки дают больше событий и суммарной длительности?
select
    place_title,
    sum(event_count) as events,
    sum(total_duration_minutes) as duration_minutes
from analytics_public.mart_daily_events
where place_title <> 'Не указано'
group by place_title
order by events desc, duration_minutes desc;

-- 3. Какая доля событий бесплатная?
select
    sum(free_event_count) as free_events,
    sum(event_count) as all_events,
    round(100.0 * sum(free_event_count) / nullif(sum(event_count), 0), 1)
        as free_share_percent
from analytics_public.mart_daily_events;

-- Независимая проверка: два повторных расписания удаляются по бизнес-ключу.
with raw_occurrences as (
    select
        source_system,
        event_id,
        period ->> 'start' as start_at
    from raw.kudago_events
    cross join lateral jsonb_array_elements(payload -> 'dates') as period
    where source_system = 'kudago'
)
select
    count(*) as raw_occurrences,
    count(distinct (source_system, event_id, start_at)) as distinct_occurrences,
    (select count(*) from analytics_candidate.fct_event_occurrence
        where source_system = 'kudago') as fact_occurrences
from raw_occurrences;

-- Проверка повторного запуска:
-- 1. Выполнить этот блок до `make pipeline`.
create temporary table mart_before as
select * from analytics_public.mart_daily_events;

-- 2. Выполнить `make pipeline`, затем запрос ниже. Ожидается 0 отличий.
select count(*) as differences
from (
    (select * from mart_before except select * from analytics_public.mart_daily_events)
    union all
    (select * from analytics_public.mart_daily_events except select * from mart_before)
) differences;
