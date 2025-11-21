{{
  config(
    materialized="table",
    schema="stage"
  )
}}

with base_stu_ed_org as (
    select * from {{ ref('edu_edfi_source', 'base_ef3__student_education_organization_associations') }}
),
keyed as (
    select 
        {{ edu_edfi_source.gen_skey('k_student') }},
        {{ edu_edfi_source.gen_skey('k_student_xyear') }},
        {{ edu_edfi_source.edorg_ref(annualize=False) }},
        api_year as school_year,
        base_stu_ed_org.*
        {{ edu_edfi_source.extract_extension(model_name=this.name, flatten=True) }}
    from base_stu_ed_org
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student, ed_org_id',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
