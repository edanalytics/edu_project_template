{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 2008 %}

with stg_bell_schedules as (
    select * from {{ ref('stg_ef3__bell_schedules_orig') }}
),
missingDates as (
    select k_bell_schedule,
        size(cast(v_dates as array<string>)) dateCount
    from stg_bell_schedules
    where size(cast(v_dates as array<string>)) = 0
)
/* Bell Schedules must have dates. */
select bs.k_bell_schedule, bs.bell_schedule_name, bs.school_id,
    {{ error_code }} as error_code,
    concat('Missing Bell Schedule DATES for Bell Schedule ', bs.bell_schedule_name, '.') as error,
    {{ error_severity_column(error_code, 'bs') }}
from stg_bell_schedules bs
join missingDates md
    on md.k_bell_schedule = bs.k_bell_schedule