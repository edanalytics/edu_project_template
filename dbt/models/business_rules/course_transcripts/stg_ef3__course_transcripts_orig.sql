{{
  config(
    materialized="table",
    schema="stage"
  )
}}

with base_course_transcripts as (
    select * from {{ ref('edu_edfi_source', 'base_ef3__course_transcripts') }}
    where not is_deleted
),
keyed as (
    select
        {{ edu_edfi_source.gen_skey('k_course') }},
        {{ edu_edfi_source.gen_skey('k_student_academic_record') }},
        base_course_transcripts.*
        {{ edu_edfi_source.extract_extension(model_name=this.name, flatten=True) }}
    from base_course_transcripts
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_course, k_student_academic_record, course_attempt_result',
            order_by='api_year desc, pull_timestamp desc'
        )
    }}
)
select * from deduped