{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3101 %}

with stg_student_school_associations as (
    select * from {{ ref('stg_ef3__student_school_associations_orig') }} ssa
    where 1=1
        {{ school_year_exists(error_code, 'ssa') }}
)
/* Enrollment Begin Date must be within the school year begin and end date. */
select ssa.k_student, ssa.k_school, ssa.k_school_calendar, ssa.school_id, ssa.student_unique_id, ssa.school_year, 
    ssa.entry_date, ssa.entry_grade_level,
    s.state_student_id as legacy_state_student_id,
    {{ error_code }} as error_code,
    concat('Student School Association Entry Date does not fall within the school year for Student ', 
        ssa.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ')',
        '. Value Received: ', ssa.entry_date, 
        '. The state school year starts ',
        concat((ssa.school_year-1), '-07-01'), ' and ends ', concat(ssa.school_year, '-06-30'), '.') as error,
    {{ error_severity_column(error_code, 'ssa') }}
from stg_student_school_associations ssa
join {{ ref('stg_ef3__students') }} s
    on s.k_student = ssa.k_student
where 
    not(ssa.entry_date between to_date(concat((ssa.school_year-1), '-07-01'), 'yyyy-MM-dd') 
        and to_date(concat(ssa.school_year, '-06-30'), 'yyyy-MM-dd'))