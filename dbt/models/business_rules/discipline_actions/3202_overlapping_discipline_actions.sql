{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3202 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
stg_discipline_actions as (
    select * 
    from {{ ref('stg_ef3__discipline_actions_orig') }} da
    where exists (
        select 1
        from brule
        where cast(da.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    )
),
calendar_dates as (
    select k_school, calendar_code, school_year, calendar_date, day_of_school_year
    from {{ ref('dim_calendar_date') }}
    where is_school_day = true
),
discipline_start_end_dates as (
    select da.k_student, da.k_school__responsibility, da.school_year,
        da.discipline_action_id, da.discipline_date, da.responsibility_school_id,
        da.student_unique_id, s.state_student_id,
        ceiling(coalesce(da.actual_discipline_action_length, da.discipline_action_length)) as discipline_action_length,
        end_cd.calendar_date as discipline_end_date
    from stg_discipline_actions da
    join {{ ref('stg_ef3__student_school_associations_orig') }} ssa
        on ssa.k_student = da.k_student
        and ssa.k_school = da.k_school__responsibility
    join {{ ref('stg_ef3__students') }} s
        on s.k_student = ssa.k_student
    join calendar_dates start_cd
        on start_cd.k_school = da.k_school__responsibility
        and start_cd.school_year = da.school_year
        and start_cd.calendar_code = ssa.calendar_code
        and start_cd.calendar_date = da.discipline_date
    join calendar_dates end_cd
        on end_cd.k_school = da.k_school__responsibility
        and end_cd.school_year = da.school_year
        and end_cd.calendar_code = ssa.calendar_code
        and end_cd.day_of_school_year = 
            ((start_cd.day_of_school_year-1) + ceiling(coalesce(da.actual_discipline_action_length, da.discipline_action_length)))
    where ifnull(ceiling(coalesce(da.actual_discipline_action_length, da.discipline_action_length)), 0) > 0
)
/* Discipline Actions are not allowed to overlap for the same student. */
select da1.k_student, da1.k_school__responsibility, da1.school_year,
    da1.discipline_action_id, da1.discipline_date, da1.responsibility_school_id,
    da1.student_unique_id,
    da1.state_student_id as legacy_state_student_id,
    brule.tdoe_error_code as error_code,
    concat('Student ', 
        da1.student_unique_id, ' (', coalesce(da1.state_student_id, '[no value]'), ') ',
        'has Discipline Actions that overlap. Discipline Action ID ', 
        da1.discipline_action_id, ' overlaps with Discipline Action ID ', da2.discipline_action_id, '.') as error,
    brule.tdoe_severity as severity
from discipline_start_end_dates da1
join discipline_start_end_dates da2
    on da2.school_year = da1.school_year
    and da2.k_student = da1.k_student
    and da2.discipline_action_id != da1.discipline_action_id
    /* Uncommenting this means conflicting rows will only show up once. But if there is an overlap between two different schools, 
        that could be a problem for the error resolution folks. */
    --and da2.discipline_date > da1.discipline_date
    /* This looks for overlapping dates. */
    and (da1.discipline_date <= da2.discipline_end_date) and (da1.discipline_end_date >= da2.discipline_date)
join brule
    on da1.school_year between brule.error_school_year_start and brule.error_school_year_end
order by da1.school_year, da1.student_unique_id, da1.k_school__responsibility