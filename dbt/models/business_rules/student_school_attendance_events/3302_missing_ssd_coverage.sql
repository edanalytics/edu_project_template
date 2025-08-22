{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3302 %}

/* A Student's SSD must cover their entire enrollment period. */
with attendance_events as (
    select *
    from {{ ref('stg_ef3__student_school_attendance_events_orig') }} ssae
    where ssae.attendance_event_category = 'Student Standard Day'
        {{ school_year_exists(error_code, 'ssae') }}
),
first_ssd_per_student as (
    select k_student, k_school, cast(school_year as int) as school_year, 
        min(attendance_event_date) as attendance_event_date
    from attendance_events 
    group by k_student, k_school, cast(school_year as int)
),
calendar_dates as (
    select cd.k_calendar_date, cd.k_school_calendar, c.k_school, cd.tenant_code,
        cd.school_year, cd.calendar_date, summarize_calendar_events.is_school_day
    from {{ ref('stg_ef3__calendar_dates_orig') }} cd
    join {{ ref('stg_ef3__calendars_orig') }} c
        on cd.k_school_calendar = c.k_school_calendar
    join (
            select 
                ce.k_calendar_date,
                -- if there are multiple events on a day, having at least one 
                -- that counts as a school day applies to the whole day
                sum(xce.is_school_day::integer) >= 1 as is_school_day
            from {{ ref('stg_ef3__calendar_dates__calendar_events_orig') }} ce
            join {{ ref('xwalk_calendar_events') }} xce
                on ce.calendar_event = xce.calendar_event_descriptor
            group by 1
        ) summarize_calendar_events
        on cd.k_calendar_date = summarize_calendar_events.k_calendar_date
    where summarize_calendar_events.is_school_day = true
),
enrollments_and_ssd_date as (
    select ssa.k_student, ssa.k_school, ssa.school_year, ssa.school_id,
        ssa.student_unique_id, ssa.entry_date, ssa.exit_withdraw_date,
        fssd.attendance_event_date,
        case
            when fssd.attendance_event_date is null then 0
            when fssd.attendance_event_date > ssa.entry_date then 0
            else 1
        end as ssd_good
    from {{ ref('stg_ef3__student_school_associations_orig') }} ssa
    join first_ssd_per_student fssd
        on fssd.k_school = ssa.k_school
        and fssd.k_student = ssa.k_student
        and fssd.school_year = ssa.school_year
    where 
        /* Enrollment dates must include at least one school day. This eliminates no shows. */
        exists (
            select 1
            from calendar_dates cd
            where cd.k_school = ssa.k_school
                and cd.school_year = ssa.school_year
                and cd.calendar_date >= ssa.entry_date
                and (ssa.exit_withdraw_date is null or cd.calendar_date < ssa.exit_withdraw_date)
        )
)
select x.k_student, x.k_school, cast(null as string) as k_session, x.school_year,
    cast(x.school_id as int) as school_id, x.student_unique_id, 
    cast(null as date) as attendance_event_date, 'SSD' as attendance_event_category,
    {{ error_code }} as error_code,
    concat('Student Standard Day missing for Student: ', x.student_unique_id, ', ', 
        'District: ', {{ get_district_from_school_id('x.school_id') }}, ', ',
        'School: ', x.school_id, ', ',
        'Enrollment Entry Date: ', x.entry_date, ', ',
        'Enrollment End Date: ', coalesce(x.exit_withdraw_date, '[null]'), ', ',
        'First SSD Date: ', coalesce(x.attendance_event_date, '[null]'), '.') as error,
    {{ error_severity_column(error_code, 'x') }}
from enrollments_and_ssd_date x
where x.ssd_good = 0