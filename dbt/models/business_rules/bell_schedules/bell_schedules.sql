{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

select *
from {{ ref('2008_dates_required') }}