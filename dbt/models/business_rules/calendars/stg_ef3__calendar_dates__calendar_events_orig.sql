{{
  config(
    materialized="table",
    schema="stage"
  )
}}

with stg_calendar_dates as (
    select * from {{ ref('stg_ef3__calendar_dates_orig') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        k_school_calendar,
        k_calendar_date,
        n_calendar_events,
        {{ edu_edfi_source.extract_descriptor('value:calendarEventDescriptor::string') }} as calendar_event
    from stg_calendar_dates
        {{ edu_edfi_source.json_flatten('v_calendar_events') }}
)
select * from flattened
