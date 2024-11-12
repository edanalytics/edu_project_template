{{
  config(
    materialized="table",
    schema="stage",
    post_hook=[
        "drop table {{ ref('stg_ef3__sessions') }}",
        "alter table {{ this }} rename to {{ ref('stg_ef3__sessions') }}"
    ]
  )
}}

with stg_sessions as (
    select * from {{ ref('stg_ef3__sessions') }}
),
errors as (
    select * from {{ ref('sessions')}}
)
select x.*
from stg_sessions x
where not exists (
    select 1
    from errors e
    where e.k_session = x.k_session
)