{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3300 %}

with stg_attendance as (
    select k_student, k_school, k_session, 
        cast(school_year as int) as school_year, 
        school_id, student_unique_id, attendance_event_date, attendance_event_category
    from {{ ref('stg_ef3__student_school_attendance_events_orig') }}
    where attendance_event_category = 'SSD'
)
/* Student Standard Day events must be within enrollment period. */
select ssd.k_student, ssd.k_school, ssd.k_session, ssd.school_year, ssd.school_id, ssd.student_unique_id,
    ssd.attendance_event_date, ssd.attendance_event_category,
    {{ error_code }} as error_code,
    concat('Student Standard Day does not fall within Enrollment Period. Enrollment Start Date: ',
        ifnull(ssa.entry_date, '[null]'), ', Enrollment End Date: ', 
        ifnull(ssa.exit_withdraw_date, '[null]'), ', Student Standard Day Effective Date: ', 
        ssd.attendance_event_date, '.') as error,
    {{ error_severity_column(error_code, 'ssd') }}
from stg_attendance ssd
left outer join {{ ref('stg_ef3__student_school_associations_orig') }} ssa
    on ssa.k_student = ssd.k_student
    and ssa.k_school = ssd.k_school
    and ssa.school_year = cast(ssd.school_year as int)
    /* No shows don't count. */
    --and ssa.entry_date < ifnull(ssa.exit_withdraw_date, to_date('9999-12-31','yyyy-MM-dd'))
where (
        ssa.k_student is null
        or (ssa.k_student is not null 
            and not(ssd.attendance_event_date between ssa.entry_date 
                and ifnull(ssa.exit_withdraw_date, to_date('9999-12-31','yyyy-MM-dd')))
            )
    )
    /* The date has to fit between some enrollment period even if it doesn't fit between EVERY enrollment 
        period for a student. */
    and not exists (
        select 1
        from {{ ref('stg_ef3__student_school_associations_orig') }} x
        where x.k_student = ssd.k_student
            and x.k_school = ssd.k_school
            and x.school_year = cast(ssd.school_year as int)
            /* No shows don't count. */
            --and x.entry_date < ifnull(x.exit_withdraw_date, to_date('9999-12-31','yyyy-MM-dd'))
            and not(ssd.attendance_event_date between x.entry_date 
                and ifnull(x.exit_withdraw_date, to_date('9999-12-31','yyyy-MM-dd')))
    )