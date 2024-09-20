/*
Find schools that are not associated with any lea.
*/
{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
with stg_school as (
    select * from {{ ref('stg_ef3__schools') }}
),
dim_lea as (
    select * from {{ ref('dim_lea') }}
)
select
    stg_school.k_school,
    stg_school.tenant_code,
    stg_school.school_id
from stg_school
left join dim_lea
    on stg_school.k_lea = dim_lea.k_lea
where dim_lea.k_lea is null