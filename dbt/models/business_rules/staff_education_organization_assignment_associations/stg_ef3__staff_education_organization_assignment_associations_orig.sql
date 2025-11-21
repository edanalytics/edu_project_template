{{
  config(
    materialized="table",
    schema="stage"
  )
}}

with base_staff_ed_org_assign as (
    select * from {{ ref('edu_edfi_source', 'base_ef3__staff_education_organization_assignment_associations') }}
),
keyed as (
    select 
        {{ edu_edfi_source.gen_skey('k_staff') }},
        {{ edu_edfi_source.edorg_ref() }},
        api_year as school_year,
        base_staff_ed_org_assign.*
        {{ edu_edfi_source.extract_extension(model_name=this.name, flatten=True) }}
    from base_staff_ed_org_assign
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by= 'tenant_code, api_year, begin_date, ed_org_id, staff_unique_id, staff_classification',
            order_by='api_year desc, last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
