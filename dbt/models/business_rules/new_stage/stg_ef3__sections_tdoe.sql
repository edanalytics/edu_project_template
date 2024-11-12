{{
  config(
    materialized="table",
    schema="stage",
    post_hook=[
        "drop table {{ ref('stg_ef3__sections') }}",
        "alter table {{ this }} rename to {{ ref('stg_ef3__sections') }}"
    ]
  )
}}

with stg_sections as (
    select * from {{ ref('stg_ef3__sections') }}
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
)