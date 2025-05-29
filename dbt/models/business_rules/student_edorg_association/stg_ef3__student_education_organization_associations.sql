{{
  config(
    materialized="table",
    schema="stage"
  )
}}

select x.*
from {{ ref('stg_ef3__student_education_organization_associations_orig') }} x
where not exists (
    select 1
    from {{ ref('student_education_organization_associations') }} e
    where ifnull(e.k_lea, 'X') = ifnull(x.k_lea, 'X')
        and ifnull(e.k_school, 'X') = ifnull(x.k_school, 'X')
        and e.k_student = x.k_student
        and e.severity = 'critical'
)