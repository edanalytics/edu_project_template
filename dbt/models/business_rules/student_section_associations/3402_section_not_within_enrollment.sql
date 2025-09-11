{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3402 %}

with stg_student_section_associations as (
    select * from {{ ref('stg_ef3__student_section_associations_orig') }} ssa
    where 1=1
        {{ school_year_exists(error_code, 'ssa') }}
)
/* Student Section Association Begin Date must be within an enrollment. */
select ssa.k_student, ssa.k_course_section, ssa.school_id, ssa.student_unique_id, ssa.local_course_code, ssa.school_year, 
    ssa.section_id, ssa.begin_date,
    s.state_student_id as legacy_state_student_id,
    {{ error_code }} as error_code,
    concat('Student Section Begin Date for Student ', 
        ssa.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
        'does not fall within student enrollment period. Student Section Begin Date: ',
        ssa.begin_date, 
        ', Enrollment Entry Date: ', ifnull(enrollments.entry_date, '[orphan (probably)]'),
        ', Exit Withdraw Date: ', ifnull(enrollments.exit_withdraw_date, '[null]'),
        '.') as error,
    {{ error_severity_column(error_code, 'ssa') }}
from stg_student_section_associations ssa
join {{ ref('stg_ef3__students') }} s
    on s.k_student = ssa.k_student
left outer join {{ ref('stg_ef3__student_school_associations_orig') }} enrollments
    on enrollments.school_year = ssa.school_year
    and enrollments.k_student = ssa.k_student
    and enrollments.school_id = ssa.school_id
where not exists (
        /* The Section begin date MUST fit within an enrollment period somewhere. */
        select 1
        from {{ ref('stg_ef3__student_school_associations_orig') }} x
        where x.school_year = ssa.school_year
            and x.k_student = ssa.k_student
            and x.school_id = ssa.school_id
            and (
                ssa.begin_date between x.entry_date and date_add(ifnull(x.exit_withdraw_date, '9999-12-31'),-1)
            )
    )