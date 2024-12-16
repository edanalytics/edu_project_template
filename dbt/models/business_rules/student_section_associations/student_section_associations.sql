{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

select *
from {{ ref('3400_begin_date') }}
union
select *
from {{ ref('3401_end_date') }}
union
select *
from {{ ref('3402_section_not_within_enrollment') }}