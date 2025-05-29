{{
  config(
    materialized="table",
    schema="stage"
  )
}}

select x.*
from {{ ref('stg_ef3__student_homeless_program_associations_orig') }} x
where not exists (
    select 1
    from {{ ref('student_homeless_program_associations') }} e
    where e.k_lea = x.k_lea
        and e.k_student = x.k_student
        and e.k_program = x.k_program
        and e.school_year = x.school_year
        and e.program_enroll_begin_date = x.program_enroll_begin_date
        and e.severity = 'critical'
)