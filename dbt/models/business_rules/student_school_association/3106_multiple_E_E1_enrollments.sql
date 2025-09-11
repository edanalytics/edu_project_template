{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3106 %}

with stg_student_school_associations as (
    select * from {{ ref('stg_ef3__student_school_associations_orig') }} ssa 
    where 1=1
        {{ school_year_exists(error_code, 'ssa') }}
)
/* Only one Enrollment per year is allowed to be E or E1 at any given school. */
select p1.k_student, p1.k_school, p1.k_school_calendar, p1.school_id, p1.student_unique_id, p1.school_year, 
    p1.entry_date, p1.entry_grade_level,
    s.state_student_id as legacy_state_student_id,
    {{ error_code }} as error_code,
    concat('Student ', 
        p1.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
        'has more than one E or E1 Enrollment in a school year. Enrollment Begin Date: ', p1.entry_date,
        ', Enrollment End Date: ', ifnull(p1.exit_withdraw_date, '[null]'), ', Enrollment Reason Code: ', p1.entry_type) as error,
    {{ error_severity_column(error_code, 'p1') }}
from stg_student_school_associations p1
join {{ ref('stg_ef3__students') }} s
    on s.k_student = p1.k_student
where p1.is_primary_school = true
    and p1.entry_type in ('E','E1')
    and exists (
        select 1
        from stg_student_school_associations p2
        where p2.is_primary_school = true
            and p2.entry_type in ('E','E1')
            and p2.school_year = p1.school_year
            and p2.student_unique_id = p1.student_unique_id
            and p2.school_id != p1.school_id
            /* This excludes same rows. */
            and not(
                p1.k_student = p2.k_student
                and p1.k_school = p2.k_school
                and p1.k_school_calendar = p2.k_school_calendar
                and p1.entry_date = p2.entry_date
            )
    )
order by p1.school_year, p1.student_unique_id, p1.entry_date