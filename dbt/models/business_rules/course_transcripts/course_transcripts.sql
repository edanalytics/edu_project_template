{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

select *
from {{ ref('3600_invalid_letter_grades') }}
union
select *
from {{ ref('3601_invalid_final_grade') }}