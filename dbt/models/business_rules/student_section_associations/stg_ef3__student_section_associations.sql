{{
  config(
    materialized="table",
    schema="stage"
  )
}}

select x.*
from {{ ref('stg_ef3__student_section_associations_orig') }} x
where not exists (
    select 1
    from {{ ref('student_section_associations') }} e
    where e.k_student = x.k_student
      and e.k_course_section = x.k_course_section
      and e.begin_date = x.begin_date
)