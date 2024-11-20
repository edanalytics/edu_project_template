{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

select *
from {{ ref('3109_nighttime_residence') }}