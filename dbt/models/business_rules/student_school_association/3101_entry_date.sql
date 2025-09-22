{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3101 %}

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
/* Enrollment Begin Date must be within the school year begin and end date. */
select ssa.k_student, ssa.k_school, ssa.k_school_calendar, ssa.school_id, ssa.student_unique_id, ssa.school_year, 
    ssa.entry_date, ssa.entry_grade_level,
    s.state_student_id as legacy_state_student_id,
    brule.tdoe_error_code as error_code,
    concat('Student School Association Entry Date does not fall within the school year for Student ', 
        ssa.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ')',
        '. Value Received: ', ssa.entry_date, 
        '. The state school year starts ',
        concat((ssa.school_year-1), '-07-01'), ' and ends ', concat(ssa.school_year, '-06-30'), '.') as error,
    brule.tdoe_severity as severity
from stg_student_school_associations ssa
join {{ ref('stg_ef3__students') }} s
    on s.k_student = ssa.k_student
join brule
    on ssa.school_year between brule.error_school_year_start and brule.error_school_year_end
where 
    not(ssa.entry_date between to_date(concat((ssa.school_year-1), '-07-01'), 'yyyy-MM-dd') 
        and to_date(concat(ssa.school_year, '-06-30'), 'yyyy-MM-dd'))