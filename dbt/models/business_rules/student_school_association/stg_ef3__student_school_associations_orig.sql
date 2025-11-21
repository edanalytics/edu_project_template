{{
  config(
    materialized="table",
    schema="stage"
  )
}}

with base_student_school_assoc as (
    select * from {{ ref('edu_edfi_source', 'base_ef3__student_school_associations') }}
),
keyed as (
    select 
        {{ edu_edfi_source.gen_skey('k_student') }},
        {{ edu_edfi_source.gen_skey('k_student_xyear') }},
        {{ edu_edfi_source.gen_skey('k_school') }},
        {{ edu_edfi_source.gen_skey('k_school_calendar') }},
        {{ edu_edfi_source.gen_skey('k_graduation_plan') }},
        base_student_school_assoc.*
        {{ edu_edfi_source.extract_extension(model_name=this.name, flatten=True) }}
    from base_student_school_assoc
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student, k_school, entry_date', 
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted