{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 1003 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
calendars as (
    select *
    from {{ ref('stg_ef3__calendars_orig') }} c
    where exists (
        select 1
        from brule
        where cast(c.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    )
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
    brule.tdoe_error_code as error_code,
    concat('Calendar Event \'', c.calendar_event, '\' on ', c.calendar_date, 
      ' does not fall within the school year. The state school year starts ',
      concat((c.school_year-1), '-07-01'), ' and ends ', concat(c.school_year, '-06-30'), '.') as error,
    brule.tdoe_severity as severity
from calendar_events c
join brule
    on c.school_year between brule.error_school_year_start and brule.error_school_year_end
where not(c.calendar_date between to_date(concat((c.school_year-1), '-07-01'), 'yyyy-MM-dd') 
    and to_date(concat(c.school_year, '-06-30'), 'yyyy-MM-dd'))
order by 3, 4, 5