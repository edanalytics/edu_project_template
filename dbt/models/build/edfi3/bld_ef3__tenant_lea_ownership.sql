{{
  config(
    materialized="table",
    schema="wh",
    alias="bld_ef3__tenant_lea_ownership"
  )
}}
-- we are not a multi-tenant configuration, so our tenant owns all our leas
select distinct tenant_code, lea_id
from {{ ref('stg_ef3__local_education_agencies') }}