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
from {{ ref('3201_discipline_action_length') }}
union
select *
from {{ ref('3202_actual_discipline_action_length') }}
union
select *
from {{ ref('3203_incidents_after_actions') }}
union
select *
from {{ ref('3204_overlapping_discipline_actions') }}