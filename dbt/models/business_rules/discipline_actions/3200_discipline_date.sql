{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3200 %}

with stg_discipline_actions as (
    select * from {{ ref('stg_ef3__discipline_actions_orig') }} da
    where 1=1
        {{ school_year_exists(error_code, 'da') }}
)
/* Discipline Date must fall within the school year. */
select da.k_student, da.k_school__responsibility, da.school_year,
    da.discipline_action_id, da.discipline_date, da.responsibility_school_id,
    da.student_unique_id,
    {{ error_code }} as error_code,
    concat('Discipline Date does not fall fall within the school year. Value Received: ', da.discipline_date, '. The state school year starts ',
      concat(cast((da.school_year-1) as int), '-07-01'), ' and ends ', concat(cast(da.school_year as int), '-06-30'), '.') as error,
    {{ error_severity_column(error_code, 'da') }}
from stg_discipline_actions da
where not(da.discipline_date between to_date(concat(cast((da.school_year-1) as int), '-07-01'), 'yyyy-MM-dd') 
    and to_date(concat(cast(da.school_year as int), '-06-30'), 'yyyy-MM-dd'))