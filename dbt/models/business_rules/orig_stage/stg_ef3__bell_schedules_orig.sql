{{
  config(
    materialized="table",
    schema="stage"
  )
}}

select * from {{ ref('stg_ef3__bell_schedules') }}