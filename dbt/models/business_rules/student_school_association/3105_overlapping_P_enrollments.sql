{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3105 %}

with stg_student_school_associations as (
    select * from {{ ref('stg_ef3__student_school_associations_orig') }} ssa
    where 1=1
        {{ school_year_exists(error_code, 'ssa') }}
)
/* Secondary enrollments require an open Primary enrollment. */
select p1.k_student, p1.k_school, p1.k_school_calendar, p1.school_id, p1.student_unique_id, p1.school_year, 
    p1.entry_date, p1.entry_grade_level,
    {{ error_code }} as error_code,
    concat('Students cannot have overlapping Primary Enrollments. Student Service School: ', p1.school_id,
        ', Enrollment Begin Date: ', p1.entry_date, ', Enrollment End Date: ', ifnull(p1.exit_withdraw_date, 'null')) as error,
    {{ error_severity_column(error_code, 'p1') }}
from stg_student_school_associations p1
where p1.is_primary_school = true
    and exists (
        select 1
        from stg_student_school_associations p2
        where p2.is_primary_school = true
            and p2.school_year = p1.school_year
            and p2.student_unique_id = p1.student_unique_id
            /* This excludes same rows. */
            and not(
                p1.k_student = p2.k_student
                and p1.k_school = p2.k_school
                and p1.k_school_calendar = p2.k_school_calendar
                and p1.entry_date = p2.entry_date
            )
            /* This looks for overlapping dates. */
            and (p1.entry_date <= ifnull(p2.exit_withdraw_date, to_date('9999-12-31', 'yyyy-MM-dd'))) 
            and (ifnull(p1.exit_withdraw_date, to_date('9999-12-31', 'yyyy-MM-dd')) >= p2.entry_date)
    )
order by p1.school_year, p1.student_unique_id, p1.entry_date