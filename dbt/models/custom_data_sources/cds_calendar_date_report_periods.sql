{{
  config(
    materialized="table",
    schema="cds"
  )
}}

with stg_calendar_date as (
    select * from {{ ref('stg_ef3__calendar_dates') }}
),
stg_calendar_events as (
    select * from {{ ref('stg_ef3__calendar_dates__calendar_events')}}
),
dim_school_calendar as (
    select * from {{ ref('dim_school_calendar') }}
),
xwalk_calendar_events as (
    select * from {{ ref('xwalk_calendar_events') }}
),
summarize_calendar_events as (
    select
        stg_calendar_events.k_calendar_date,
        -- if there are multiple events on a day, having at least one
        -- that counts as a school day applies to the whole day
        sum(xwalk_calendar_events.is_school_day::integer) >= {{ var("edu:attendance:num_school_day_calendar_events", 1) }} as is_school_day
    from stg_calendar_events
    join xwalk_calendar_events
        on stg_calendar_events.calendar_event = xwalk_calendar_events.calendar_event_descriptor
    group by 1
),
formatted as (
    select
        stg_calendar_date.k_calendar_date,
        stg_calendar_date.k_school_calendar,
        dim_school_calendar.k_school,
        stg_calendar_date.tenant_code,
        stg_calendar_date.school_year,
        stg_calendar_date.calendar_date,
        summarize_calendar_events.is_school_day
    from stg_calendar_date
    join dim_school_calendar
        on stg_calendar_date.k_school_calendar = dim_school_calendar.k_school_calendar
    join summarize_calendar_events
        on stg_calendar_date.k_calendar_date = summarize_calendar_events.k_calendar_date
)
select k_calendar_date,
    report_period,
    row_number() over (
        partition by k_school_calendar, report_period
        order by calendar_date) day_of_report_period,
    min(calendar_date) over (
        partition by k_school_calendar, report_period) as report_period_begin_date,
    max(calendar_date) over (
        partition by k_school_calendar, report_period) as report_period_end_date,
    count(*) over (
        partition by k_school_calendar, report_period
        rows between unbounded preceding and unbounded following) as days_in_report_period
from (
    select k_calendar_date, k_school_calendar, calendar_date,
        case
            when report_period <= 9 then report_period
            else 9
        end as report_period
    from (
        select k_calendar_date, k_school_calendar, calendar_date,
            ceiling(row_number() over (
                partition by k_school_calendar, is_school_day
                order by calendar_date) / 20) as report_period
        from formatted
        where is_school_day = true
    ) x
) x

