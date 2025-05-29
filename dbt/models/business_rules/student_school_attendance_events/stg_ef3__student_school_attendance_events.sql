{{
  config(
    materialized="table",
    schema="stage"
  )
}}

select x.*
from {{ ref('stg_ef3__student_school_attendance_events_orig') }} x
where not exists (
    select 1
    from {{ ref('student_school_attendance_events') }} e
    where e.k_school = x.k_school
        and e.k_student = x.k_student
        and e.k_session = x.k_session
        and e.attendance_event_category = x.attendance_event_category
        and e.attendance_event_date = x.attendance_event_date
        and e.severity = 'critical'
)