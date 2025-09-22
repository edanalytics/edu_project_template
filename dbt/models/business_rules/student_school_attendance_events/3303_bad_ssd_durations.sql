{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3303 %}

/* A Student's SSD duration must be non-zero. */
with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
attendance_events as (
    select k_student, k_school, k_session, cast(school_year as int) as school_year,
        school_id, student_unique_id, attendance_event_date, attendance_event_category
    from {{ ref('stg_ef3__student_school_attendance_events_orig') }} ssae
    where ssae.attendance_event_category = 'Student Standard Day'
        and coalesce(ssae.school_attendance_duration,0) = 0
        and exists (
        select 1
        from brule
        where cast(ssae.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    )
)
select x.k_student, x.k_school, x.k_session, x.school_year,
    cast(x.school_id as int) as school_id, x.student_unique_id, 
    x.attendance_event_date, x.attendance_event_category,
    s.state_student_id as legacy_state_student_id,
    brule.tdoe_error_code as error_code,
    concat('SSD Duration missing for Student: ', x.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]') ,'), ',
        'District: ', {{ get_district_from_school_id('x.school_id') }}, ', ',
        'School: ', x.school_id, ', ',
        'Enrollment Entry Date: ', ssa.entry_date, ', ',
        'Enrollment End Date: ', coalesce(ssa.exit_withdraw_date, '[null]'), '.') as error,
    brule.tdoe_severity as severity
from attendance_events x
join {{ ref('stg_ef3__student_school_associations_orig') }} ssa
    on ssa.k_school = x.k_school
    and ssa.k_student = x.k_student
    and ssa.school_year = cast(x.school_year as int)
join {{ ref('stg_ef3__students') }} s
    on s.k_student = ssa.k_student
join brule
    on x.school_year between brule.error_school_year_start and brule.error_school_year_end