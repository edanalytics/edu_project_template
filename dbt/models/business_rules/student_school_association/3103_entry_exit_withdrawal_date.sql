{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3103 %}

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
/* Withdrawal Date must be greater than or equal to entry date. */
select ssa.k_student, ssa.k_school, ssa.k_school_calendar, ssa.school_id, ssa.student_unique_id, ssa.school_year, 
    ssa.entry_date, ssa.entry_grade_level,
    s.state_student_id as legacy_state_student_id,
    brule.tdoe_error_code as error_code,
    concat('Exit Withdrawal Date for Student ', 
        ssa.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
        'must be greater than or equal to the Entry Date. Exit Withdrawal Date received: ',
        ssa.exit_withdraw_date, ', Entry Date: ', ssa.entry_date, '.') as error,
    brule.tdoe_severity as severity
from stg_student_school_associations ssa
join {{ ref('stg_ef3__students') }} s
    on s.k_student = ssa.k_student
join brule
    on ssa.school_year between brule.error_school_year_start and brule.error_school_year_end
where ssa.exit_withdraw_date is not null
    and ssa.exit_withdraw_date < ssa.entry_date