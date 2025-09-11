{{
  config(
    materialized="table",
    schema="stg_adm_gaps"
  )
}}

select e.school_year, e.k_student, e.k_school, e.is_primary_school,
    'student standard day' as reason_type,
    reason_count,
    concat('Student has the following SSD errors:\n', x.errors) as possible_reason
from {{ ref('adm_gaps_enrollments') }} e
join (
        select school_year, k_student, k_school,
            count(*) as reason_count, 
            concat_ws('\n', collect_list(concat('\t', error))) as errors
        from {{ ref('student_school_attendance_events') }} x
        where attendance_event_category = 'Student Standard Day'
            and severity = 'critical'
        group by school_year, k_student, k_school
    ) x
    on x.school_year = e.school_year
    and x.k_school = e.k_school
    and x.k_student = e.k_student