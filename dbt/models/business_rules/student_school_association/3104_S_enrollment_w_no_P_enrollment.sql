{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3104 %}

with stg_student_school_associations as (
    select * from {{ ref('stg_ef3__student_school_associations_orig') }} ssa
    where 1=1
        {{ school_year_exists(error_code, 'ssa') }}
)
/* Secondary enrollments require an open Primary enrollment. */
select s.k_student, s.k_school, s.k_school_calendar, s.school_id, s.student_unique_id, s.school_year, 
    s.entry_date, s.entry_grade_level,
    {{ error_code }} as error_code,
    concat('No Active Primary Enrollment designated for Student. Student Service School: ', s.school_id) as error,
    {{ error_severity_column(error_code, 's') }}
from stg_student_school_associations s
where s.is_primary_school = false
    and not exists (
        select 1
        from stg_student_school_associations p
        where p.is_primary_school = true
            and p.school_year = s.school_year
            and p.student_unique_id = s.student_unique_id
            and s.entry_date >= p.entry_date
            and ifnull(s.exit_withdraw_date, to_date('9999-12-31', 'yyyy-MM-dd')) <=
                ifnull(p.exit_withdraw_date, to_date('9999-12-31', 'yyyy-MM-dd'))
    )