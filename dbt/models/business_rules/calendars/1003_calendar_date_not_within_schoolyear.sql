{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 1003 %}

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
)
/* Calendar Events must be within the school year */
select c.k_school, c.k_school_calendar, c.school_year, c.school_id, c.calendar_code, 
    {{ error_code }} as error_code,
    concat('Calendar Event \'', c.calendar_event, '\' on ', c.calendar_date, 
      ' does not fall within the school year. The state school year starts ',
      concat((c.school_year-1), '-07-01'), ' and ends ', concat(c.school_year, '-06-30'), '.') as error,
    {{ error_severity_column(error_code, 'c') }}
from calendar_events c
where not(c.calendar_date between to_date(concat((c.school_year-1), '-07-01'), 'yyyy-MM-dd') 
    and to_date(concat(c.school_year, '-06-30'), 'yyyy-MM-dd'))
order by 3, 4, 5