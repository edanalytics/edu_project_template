{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

select *
from {{ ref('4100_full_time_equivalency_values') }}
union
select *
from {{ ref('4101_order_of_assignment_values') }}