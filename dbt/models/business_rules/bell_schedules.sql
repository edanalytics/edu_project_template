{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

with stg_bell_schedules as (
    select * from {{ ref('stg_ef3__bell_schedules') }}
),
missingDates as (
    select k_bell_schedule,
        size(cast(v_dates as array<string>)) dateCount
    from stg_bell_schedules
    where size(cast(v_dates as array<string>)) = 0
)
/* Bell Schedules must have dates. */
select bs.k_bell_schedule, bs.bell_schedule_name, bs.school_id,
    2008 as error_code,
    concat('Missing Bell Schedule DATES for Bell Schedule ', bs.bell_schedule_name, '.') as error
from stg_bell_schedules bs
join missingDates md
    on md.k_bell_schedule = bs.k_bell_schedule