{{
  config(
    materialized="table",
    schema="stage"
  )
}}

with stg_class_periods as (
    select * from {{ ref('stg_ef3__class_periods_orig') }}
),
errors as (
    select * from {{ ref('class_periods')}}
)
select x.*
from stg_class_periods x
where not exists (
    select 1
    from errors e
    where e.k_class_period = x.k_class_period
)