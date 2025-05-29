{{
  config(
    materialized="table",
    schema="stage"
  )
}}

with stg_bell_schedules as (
    select * from {{ ref('stg_ef3__bell_schedules_orig') }}
),
errors as (
    select * from {{ ref('bell_schedules')}}
)
select x.*
from stg_bell_schedules x
where not exists (
    select 1
    from errors e
    where e.k_bell_schedule = x.k_bell_schedule
        and e.severity = 'critical'
)