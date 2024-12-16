{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3401 %}

with stg_student_section_associations as (
    select * from {{ ref('stg_ef3__student_section_associations_orig') }} ssa
    where 1=1
        {{ school_year_exists(error_code, 'ssa') }}
)
/* Student Section End Date must be within the school year begin and end date. */
select ssa.k_student, ssa.k_course_section, ssa.school_id, ssa.student_unique_id, ssa.local_course_code, ssa.school_year, 
    ssa.section_id, ssa.begin_date,
    {{ error_code }} as error_code,
    concat('Student Section Association End Date does not fall within the school year. Value Received: ', ssa.end_date, 
        '. The state school year starts ',
        concat((ssa.school_year-1), '-07-01'), ' and ends ', concat(ssa.school_year, '-06-30'), '.') as error,
    {{ error_severity_column(error_code, 'ssa') }}
from stg_student_section_associations ssa
where ssa.end_date is not null
    and not(ssa.end_date between to_date(concat((ssa.school_year-1), '-07-01'), 'yyyy-MM-dd') 
        and to_date(concat(ssa.school_year, '-06-30'), 'yyyy-MM-dd'))