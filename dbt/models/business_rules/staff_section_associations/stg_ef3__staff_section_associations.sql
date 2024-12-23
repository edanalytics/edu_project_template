{{
  config(
    materialized="table",
    schema="stage"
  )
}}

select x.*
from {{ ref('stg_ef3__staff_section_associations_orig') }} x
where not exists (
    select 1
    from {{ ref('staff_section_associations') }} e
    where e.k_staff = x.k_staff
        and e.k_course_section = x.k_course_section
)