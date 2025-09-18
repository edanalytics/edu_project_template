{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 1002 %}

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
events_incorrectly_paired_with_instructional as (
    select ce.k_school, ce.k_school_calendar, ce.school_year, ce.school_id, ce.calendar_code, ce.calendar_date, 
        ce.calendar_event
    from calendar_events ce
    where calendar_event in ('CH', 'MI', 'OH', 'SH', 'SI', 'TV')
        and exists (
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
    brule.tdoe_error_code as error_code,
    concat('Calendar Event \'', x.calendar_event, '\' on ', x.calendar_date, ' currently has an \'ID\' Calendar Event on the same date but should not.') as error,
    brule.tdoe_severity as severity
from calendars c
join events_incorrectly_paired_with_instructional x
    on x.k_school = c.k_school
    and x.k_school_calendar = c.k_school_calendar
join brule
    on c.school_year between brule.error_school_year_start and brule.error_school_year_end
order by 3, 4, 5