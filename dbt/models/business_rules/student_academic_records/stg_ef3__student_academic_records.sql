{{
  config(
    materialized="table",
    schema="stage"
  )
}}

select x.*
from {{ ref('stg_ef3__student_academic_records_orig') }} x
where not exists (
    select 1
    from {{ ref('student_academic_records') }} e
    where e.k_student_academic_record = x.k_student_academic_record
        and e.severity = 'critical'
)