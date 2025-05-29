{{
  config(
    materialized="table",
    schema="stage"
  )
}}

with stg_sections as (
    select * from {{ ref('stg_ef3__sections_orig') }}
),
errors as (
    select * from {{ ref('sections')}}
)
select x.*
from stg_sections x
where not exists (
    select 1
    from errors e
    where e.k_course_section = x.k_course_section
        and e.severity = 'critical'
)