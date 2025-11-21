{{
  config(
    materialized="table",
    schema="stage"
  )
}}

with base_stu_programs as (
    select * from {{ ref('edu_edfi_source', 'base_ef3__student_homeless_program_associations') }}
),

keyed as (
    select 
        {{ edu_edfi_source.gen_skey('k_student') }},
        {{ edu_edfi_source.gen_skey('k_student_xyear') }},
        {{ edu_edfi_source.gen_skey('k_program') }},
        {{ edu_edfi_source.edorg_ref(annualize=False) }},
        api_year as school_year,
        base_stu_programs.*
        {{ edu_edfi_source.extract_extension(model_name=this.name, flatten=True) }}

    from base_stu_programs
),

deduped as (
    {{ dbt_utils.deduplicate(
        relation='keyed',
        partition_by='k_student, k_program, program_enroll_begin_date, school_year',
        order_by='last_modified_timestamp desc, pull_timestamp desc'
    ) }}
)

select * from deduped
where not is_deleted
