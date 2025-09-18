{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 1008 %}

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
),
too_many_dates as (
    select k_school, k_school_calendar, school_year, school_id, calendar_code, count(*) as discrectionary_days
    from calendar_events
    where calendar_event in ('OV', 'OS', 'OA', 'OI', 'OO')
    group by k_school, k_school_calendar, school_year, school_id, calendar_code
    having count(*) > 4
)
/* There cannot be more than 4 discrectionary days. */
select c.k_school, c.k_school_calendar, c.school_year, c.school_id, c.calendar_code, 
    brule.tdoe_error_code as error_code,
    concat('Calculated total discrectionary days is more than the maximum of 4. Total days calculated: ',
      x.discrectionary_days, '.') as error,
    brule.tdoe_severity as severity
from calendars c
join too_many_dates x
    on x.k_school = c.k_school
    and x.k_school_calendar = c.k_school_calendar
join brule
    on c.school_year between brule.error_school_year_start and brule.error_school_year_end
order by 3, 4, 5