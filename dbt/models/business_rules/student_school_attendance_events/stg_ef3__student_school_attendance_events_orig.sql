{{
  config(
    materialized="table",
    schema="stage"
  )
}}

with base_student_school_attend as (
    select * from {{ ref('edu_edfi_source', 'base_ef3__student_school_attendance_events') }}
    where not is_deleted
),
keyed as (
    select 
        {{ edu_edfi_source.gen_skey('k_student') }},
        {{ edu_edfi_source.gen_skey('k_student_xyear') }},
        {{ edu_edfi_source.gen_skey('k_school') }},
        {{ edu_edfi_source.gen_skey('k_session') }},
        base_student_school_attend.*
        {{ edu_edfi_source.extract_extension(model_name=this.name, flatten=True) }}
    from base_student_school_attend
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student, k_school, k_session, attendance_event_category, attendance_event_date',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped
