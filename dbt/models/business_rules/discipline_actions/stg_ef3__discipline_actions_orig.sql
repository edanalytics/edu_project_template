{{
  config(
    materialized="table",
    schema="stage"
  )
}}

with base_discipline_actions as (
    select * from {{ ref('edu_edfi_source', 'base_ef3__discipline_actions') }}
),
keyed as (
    select 
        {{ edu_edfi_source.gen_skey('k_student') }},
        {{ edu_edfi_source.gen_skey('k_student_xyear') }},
        {{ edu_edfi_source.gen_skey('k_school', alt_ref='assignment_school_reference', alt_k_name='k_school__assignment') }},
        {{ edu_edfi_source.gen_skey('k_school', alt_ref='responsibility_school_reference', alt_k_name='k_school__responsibility') }},
        api_year as school_year,
        base_discipline_actions.*
        {{ edu_edfi_source.extract_extension(model_name=this.name, flatten=True) }}
    from base_discipline_actions
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='discipline_action_id, discipline_date, k_student',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted