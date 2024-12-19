{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

select *
from {{ ref('3200_discipline_date') }}
union
select *
from {{ ref('3201_incidents_after_actions') }}
union
select *
from {{ ref('3202_overlapping_discipline_actions') }}