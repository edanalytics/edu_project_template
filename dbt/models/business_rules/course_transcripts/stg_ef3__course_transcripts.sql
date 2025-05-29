{{
  config(
    materialized="table",
    schema="stage"
  )
}}

select x.*
from {{ ref('stg_ef3__course_transcripts_orig') }} x
where not exists (
    select 1
    from {{ ref('course_transcripts') }} e
    where e.k_course = x.k_course
        and e.k_student_academic_record = x.k_student_academic_record
        and e.severity = 'critical'
)