{{
  config(
    materialized="table",
    schema="stage"
  )
}}

with base_calendar_dates as (
    select * from {{ ref('edu_edfi_source', 'base_ef3__calendar_dates') }}
    where not is_deleted
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
            'lower(calendar_code)',
            'calendar_date',
            'school_id',
            'school_year']
        ) }} as k_calendar_date,
        {{ edu_edfi_source.gen_skey('k_school_calendar') }},
        base_calendar_dates.*
        {{ edu_edfi_source.extract_extension(model_name=this.name, flatten=True) }}
    from base_calendar_dates
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_calendar_date',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped