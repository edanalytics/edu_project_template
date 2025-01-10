{{
  config(
    materialized="table",
    schema="cds"
  )
}}

select stg_staff.k_staff,
    stg_staff.record_guid as oid
from {{ ref('stg_ef3__staffs') }} stg_staff