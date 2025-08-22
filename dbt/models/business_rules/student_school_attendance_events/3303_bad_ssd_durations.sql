{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3303 %}

/* A Student's SSD duration must be non-zero. */
with attendance_events as (
    select k_student, k_school, k_session, cast(school_year as int) as school_year,
        school_id, student_unique_id, attendance_event_date, attendance_event_category
    from {{ ref('stg_ef3__student_school_attendance_events_orig') }} ssae
    where ssae.attendance_event_category = 'Student Standard Day'
        and coalesce(ssae.school_attendance_duration,0) = 0
        {{ school_year_exists(error_code, 'ssae') }}
)
select x.k_student, x.k_school, x.k_session, x.school_year,
    cast(x.school_id as int) as school_id, x.student_unique_id, 
    x.attendance_event_date, x.attendance_event_category,
    {{ error_code }} as error_code,
    concat('SSD Duration missing for Student: ', x.student_unique_id, ', ',
        'District: ', {{ get_district_from_school_id('x.school_id') }}, ', ',
        'School: ', x.school_id, ', ',
        'Enrollment Entry Date: ', ssa.entry_date, ', ',
        'Enrollment End Date: ', coalesce(ssa.exit_withdraw_date, '[null]'), '.') as error,
    {{ error_severity_column(error_code, 'x') }}
from attendance_events x
join teds_dev.dev_smckee_stage.stg_ef3__student_school_associations_orig ssa
    on ssa.k_school = x.k_school
    and ssa.k_student = x.k_student
    and ssa.school_year = cast(x.school_year as int)