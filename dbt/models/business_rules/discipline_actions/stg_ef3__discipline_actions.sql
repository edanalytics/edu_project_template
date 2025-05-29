{{
  config(
    materialized="table",
    schema="stage"
  )
}}

with stg_discipline_actions as (
    select * from {{ ref('stg_ef3__discipline_actions_orig') }}
),
errors as (
    select * from {{ ref('discipline_actions')}}
)
select x.*
from stg_discipline_actions x
where not exists (
    select 1
    from errors e
    where e.k_student = x.k_student
        and e.k_school__responsibility = x.k_school__responsibility
        and e.school_year = x.school_year
        and e.discipline_action_id = x.discipline_action_id
        and e.discipline_date = x.discipline_date
        and e.severity = 'critical'
)