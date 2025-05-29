{{
  config(
    materialized="table",
    schema="stage"
  )
}}

with stg_calendars as (
    select * from {{ ref('stg_ef3__calendar_dates__calendar_events_orig') }}
),
errors as (
    select * from {{ ref('calendars')}}
)
select x.*
from stg_calendars x
where not exists (
    select 1
    from errors e
    where e.k_school_calendar = x.k_school_calendar
        and e.severity = 'critical'
)