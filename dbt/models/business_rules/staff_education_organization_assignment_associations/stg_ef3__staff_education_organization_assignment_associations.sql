{{
  config(
    materialized="table",
    schema="stage"
  )
}}

select x.*
from {{ ref('stg_ef3__staff_education_organization_assignment_associations_orig') }} x
where not exists (
    select 1
    from {{ ref('staff_education_organization_assignment_associations') }} e
    where e.k_staff = x.k_staff
      and ((e.k_lea is not null and e.k_lea = x.k_lea)
          or (e.k_school is not null and e.k_school = x.k_school))
      and e.school_year = x.school_year
      and e.begin_date = x.begin_date
      and e.staff_classification = x.staff_classification
)