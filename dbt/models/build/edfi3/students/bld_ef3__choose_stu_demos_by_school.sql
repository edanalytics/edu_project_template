{{
  config(
    materialized="table",
    schema="build"
  )
}}

select *
from {{ ref('stg_ef3__student_education_organization_associations') }}
where k_school is not null