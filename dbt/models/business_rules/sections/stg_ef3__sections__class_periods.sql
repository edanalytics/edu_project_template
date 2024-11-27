{{
  config(
    materialized="table",
    schema="stage"
  )
}}

with stg_sections as (
    select * from {{ ref('stg_ef3__sections_orig') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_course_section,
        {{ edu_edfi_source.gen_skey('k_class_period', alt_ref='value:classPeriodReference') }}
    from stg_sections,
        lateral variant_explode(v_class_periods)
)
select * from flattened