{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 1001 %}

with calendars as (
    select *
    from {{ ref('stg_ef3__calendars_orig') }} c
    where 1=1
        {{ school_year_exists(error_code, 'c') }}
),
calendar_events as (
    select c.k_school, c.k_school_calendar, cd.k_calendar_date, c.tenant_code, c.api_year, c.school_year,
        c.school_id, c.calendar_code, cd.calendar_date, ce.calendar_event
    from calendars c
    left outer join {{ ref('stg_ef3__calendar_dates_orig') }} cd
        on cd.k_school_calendar = c.k_school_calendar
    left outer join {{ ref('stg_ef3__calendar_dates__calendar_events_orig') }} ce
        on ce.k_school_calendar = cd.k_school_calendar
        and ce.k_calendar_date = cd.k_calendar_date
),
events_not_paired_as_instructional as (
    select ce.k_school, ce.k_school_calendar, ce.school_year, ce.school_id, ce.calendar_code, ce.calendar_date, 
        ce.calendar_event
    from calendar_events ce
    where calendar_event in ('AS', 'AE', 'AD', 'MS', 'MD', 'MA', 'MU', 'MH')
        and not exists (
            select 1
            from calendar_events x
            where x.k_school = ce.k_school
                and x.k_school_calendar = ce.k_school_calendar
                and x.calendar_code = ce.calendar_code
                and x.calendar_date = ce.calendar_date
                and x.calendar_event = 'ID'
        )
)
/* Some calendar events which are also instructional days must be marked as so. */
select c.k_school, c.k_school_calendar, c.school_year, c.school_id, c.calendar_code, 
    {{ error_code }} as error_code,
    concat('Calendar ', c.calendar_code, ' has calendar Event \'', x.calendar_event, '\' on ', x.calendar_date, ' must also have an \'ID\' Calendar Event on the same date.') as error,
    {{ error_severity_column(error_code, 'c') }}
from calendars c
join events_not_paired_as_instructional x
    on x.k_school = c.k_school
    and x.k_school_calendar = c.k_school_calendar
order by 3, 4, 5