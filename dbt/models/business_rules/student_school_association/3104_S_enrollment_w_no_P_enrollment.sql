{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3104 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
stg_student_school_associations as (
    select * from {{ ref('stg_ef3__student_school_associations_orig') }} ssa
    where exists (
        select 1
        from brule
        where cast(ssa.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    )
)
/* Secondary enrollments require an open Primary enrollment. */
select s.k_student, s.k_school, s.k_school_calendar, s.school_id, s.student_unique_id, s.school_year, 
    s.entry_date, s.entry_grade_level,
    s2.state_student_id as legacy_state_student_id,
    brule.tdoe_error_code as error_code,
    concat('No Active Primary Enrollment designated for Student ', 
        s.student_unique_id, ' (', coalesce(s2.state_student_id, '[no value]'), ')',
        '. Student Service School: ', s.school_id) as error,
    brule.tdoe_severity as severity
from stg_student_school_associations s
join {{ ref('stg_ef3__students') }} s2
    on s2.k_student = s.k_student
join brule
    on s.school_year between brule.error_school_year_start and brule.error_school_year_end
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