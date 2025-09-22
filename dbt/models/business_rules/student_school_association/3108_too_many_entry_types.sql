{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3108 %}

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
),
count_distinct_entry_types as (
    select k_student, k_school, school_year, count(distinct entry_type) as distinct_entry_types
    from stg_student_school_associations
    group by k_student, k_school, school_year
    having count(distinct entry_type) > 1
)
/* Students cannot have more than one entry type per school per year. */
select ssa.k_student, ssa.k_school, ssa.k_school_calendar, ssa.school_id, ssa.student_unique_id, ssa.school_year, 
    ssa.entry_date, ssa.entry_grade_level,
    s.state_student_id as legacy_state_student_id,
    brule.tdoe_error_code as error_code,
    concat('Student ', 
        ssa.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
        'has different Enrollment Entry Types for the same school for the same school year. Enrollment Begin Date: ', ssa.entry_date,
        ', Enrollment End Date: ', ifnull(ssa.exit_withdraw_date, '[null]'), ', Enrollment Reason Code: ', ssa.entry_type) as error,
    brule.tdoe_severity as severity
from stg_student_school_associations ssa
join {{ ref('stg_ef3__students') }} s
    on s.k_student = ssa.k_student
join count_distinct_entry_types x
    on x.k_school = ssa.k_school
    and x.k_student = ssa.k_student
    and x.school_year = ssa.school_year
join brule
    on ssa.school_year between brule.error_school_year_start and brule.error_school_year_end
order by ssa.school_year, ssa.student_unique_id, ssa.entry_date