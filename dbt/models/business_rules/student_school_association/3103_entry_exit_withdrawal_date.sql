{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3103 %}

with stg_student_school_associations as (
    select * from {{ ref('stg_ef3__student_school_associations_orig') }} ssa
    where 1=1
        {{ school_year_exists(error_code, 'ssa') }}
)
/* Withdrawal Date must be greater than or equal to entry date. */
select ssa.k_student, ssa.k_school, ssa.k_school_calendar, ssa.school_id, ssa.student_unique_id, ssa.school_year, 
    ssa.entry_date, ssa.entry_grade_level,
    {{ error_code }} as error_code,
    concat('Exit Withdrawal Date must be greater than or equal to the Entry Date. Exit Withdrawal Date received: ',
        ssa.exit_withdraw_date, ', Entry Date: ', ssa.entry_date, '.') as error,
    {{ error_severity_column(error_code, 'ssa') }}
from stg_student_school_associations ssa
where ssa.exit_withdraw_date is not null
    and ssa.exit_withdraw_date < ssa.entry_date