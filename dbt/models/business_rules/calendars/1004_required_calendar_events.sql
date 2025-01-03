{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 1004 %}

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
required_events as (
    select 'AS' as required_calendar_event union
    select 'AE' as required_calendar_event union
    select 'CS' as required_calendar_event union
    select 'CE' as required_calendar_event union
    select 'CH' as required_calendar_event union
    select 'SH' as required_calendar_event 
),
missing_events as (
    select k_school, k_school_calendar, school_year, school_id, calendar_code,
        substr(missing_calendar_events, 2, len(missing_calendar_events)-2) as missing_calendar_events
    from (
        select c.k_school, c.k_school_calendar, c.school_year, c.school_id, c.calendar_code,
            cast(array_agg(re.required_calendar_event) as String) as missing_calendar_events
        from calendars c
        cross join required_events re
        where not exists (
            select 1
            from calendar_events x
            where x.k_school = c.k_school
                and x.k_school_calendar = c.k_school_calendar
                and x.calendar_event = re.required_calendar_event
        )
        group by c.k_school, c.k_school_calendar, c.school_year, c.school_id, c.calendar_code
    )
)
/* Some Calendar Events are required on every calendar. */
select c.k_school, c.k_school_calendar, c.school_year, c.school_id, c.calendar_code, 
    {{ error_code }} as error_code,
    concat('This School Calendar is missing the following required events: ', x.missing_calendar_events, '.') as error,
    {{ error_severity_column(error_code, 'c') }}
from calendars c
join missing_events x
    on x.k_school = c.k_school
    and x.k_school_calendar = c.k_school_calendar
order by 3, 4, 5