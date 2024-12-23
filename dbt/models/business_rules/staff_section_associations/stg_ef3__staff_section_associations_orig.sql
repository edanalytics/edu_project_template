{{
  config(
    materialized="table",
    schema="stage"
  )
}}

with base_staff_section_assoc as (
    select * from {{ ref('edu_edfi_source', 'base_ef3__staff_section_associations') }}
    where not is_deleted
),
keyed as (
    select 
        {{ edu_edfi_source.gen_skey('k_staff') }},
        {{ edu_edfi_source.gen_skey('k_course_section') }},
        base_staff_section_assoc.*
        {{ edu_edfi_source.extract_extension(model_name=this.name, flatten=True) }}
    from base_staff_section_assoc
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_staff, k_course_section',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped