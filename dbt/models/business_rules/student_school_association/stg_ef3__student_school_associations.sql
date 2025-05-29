{{
  config(
    materialized="table",
    schema="stage"
  )
}}

select x.*
from {{ ref('stg_ef3__student_school_associations_orig') }} x
where not exists (
    select 1
    from {{ ref('student_school_associations') }} e
    where e.k_school = x.k_school
        and e.k_student = x.k_student
        and e.k_school_calendar = x.k_school_calendar
        and e.entry_date = x.entry_date
        and e.severity = 'critical'
)