{{
  config(
    materialized="table",
    schema="stage"
  )
}}

with base_student_section as (
    select * from {{ ref('edu_edfi_source', 'base_ef3__student_section_associations') }}
    where not is_deleted
),
keyed as (
    select 
        {{ edu_edfi_source.gen_skey('k_student') }},
        {{ edu_edfi_source.gen_skey('k_student_xyear') }},
        {{ edu_edfi_source.gen_skey('k_course_section') }},
        base_student_section.*
        {{ edu_edfi_source.extract_extension(model_name=this.name, flatten=True) }}
    from base_student_section
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student, k_course_section, begin_date',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped