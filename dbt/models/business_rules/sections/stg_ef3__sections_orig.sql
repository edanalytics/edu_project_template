{{
  config(
    materialized="table",
    schema="stage"
  )
}}

with base_sections as (
    select * from {{ ref('edu_edfi_source', 'base_ef3__sections') }}
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'lower(local_course_code)',
                'school_id',
                'school_year',
                'lower(section_id)',
                'lower(session_name)'
            ]
        ) }} as k_course_section,
        {{ edu_edfi_source.gen_skey('k_course_offering') }},
        -- pull k_school from the course offering definition.
        -- this is the school officially offering the course
        {{ edu_edfi_source.gen_skey('k_school', alt_ref='course_offering_reference') }},
        {{ edu_edfi_source.gen_skey('k_location') }},
        -- pull a separate k_school from location school
        -- this is the physical location where the class is taught, 
        -- which could theoretically be different from the school offering the section
        {{ edu_edfi_source.gen_skey('k_school', alt_ref='location_school_reference', alt_k_name='k_school__location') }},
        base_sections.*
        {{ edu_edfi_source.extract_extension(model_name=this.name, flatten=True) }}
    from base_sections
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_course_section',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}

)
select * from deduped
where not is_deleted
