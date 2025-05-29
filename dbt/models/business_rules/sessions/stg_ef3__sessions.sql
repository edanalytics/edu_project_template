{{
  config(
    materialized="table",
    schema="stage"
  )
}}

select x.*
from {{ ref('stg_ef3__sessions_orig') }} x
where not exists (
    select 1
    from {{ ref('sessions') }} e
    where e.k_session = x.k_session
        and e.severity = 'critical'
)