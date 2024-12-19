{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

select *
from {{ ref('3500_completion_date_not_in_schoolyear') }}