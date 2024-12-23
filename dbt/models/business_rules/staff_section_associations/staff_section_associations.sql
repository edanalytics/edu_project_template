{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

select *
from {{ ref('4200_begin_date') }}
union
select *
from {{ ref('4201_end_date') }}
union
select *
from {{ ref('4202_begin_with_end_invalid') }}