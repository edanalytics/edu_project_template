{{
  config(
    materialized="table",
    schema="stage",
    post_hook=[
        "drop table {{ ref('stg_ef3__class_periods') }}",
        "alter table {{ this }} rename to {{ ref('stg_ef3__class_periods') }}"
    ]
  )
}}

with stg_class_periods as (
    select * from {{ ref('stg_ef3__class_periods') }}
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