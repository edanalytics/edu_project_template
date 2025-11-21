{{
  config(
    materialized="table",
    schema="stage"
  )
}}

with base_calendars as (
    select * from {{ ref('edu_edfi_source', 'base_ef3__calendars') }}
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'lower(calendar_code)',
                'school_id',
                'school_year'
            ]
        ) }} as k_school_calendar,
        {{ edu_edfi_source.gen_skey('k_school') }},
        base_calendars.*
        {{ edu_edfi_source.extract_extension(model_name=this.name, flatten=True) }}
    from base_calendars
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_school_calendar',
            order_by='api_year desc, last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
