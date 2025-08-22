{{
  config(
    materialized="table",
    schema="cds"
  )
}}

with disciplinary_start_dates as (
    select da.school_year, da.tenant_code, da.k_student, da.k_school__responsibility, ssa.k_school_calendar,
        da.discipline_date, da.discipline_action_id,
        coalesce(da.actual_discipline_action_length, da.discipline_action_length) as discipline_action_length,
        min(start_cd.calendar_date) as first_possible_discipinary_date, 
        min(start_cd.day_of_school_year) as first_possible_discipinary_day
    from {{ ref('stg_ef3__discipline_actions_orig') }} da
    left outer join {{ ref('stg_ef3__student_school_associations_orig') }} ssa
        on da.school_year = ssa.school_year
        and da.tenant_code = ssa.tenant_code
        and da.k_school__responsibility = ssa.k_school
        and da.k_student = ssa.k_student
    left outer join {{ ref('dim_calendar_date') }} start_cd
        on start_cd.school_year = ssa.school_year
        and start_cd.tenant_code = ssa.tenant_code
        and start_cd.k_school_calendar = ssa.k_school_calendar
        and start_cd.is_school_day is true
        and start_cd.calendar_date >= da.discipline_date
    where ifnull(coalesce(da.actual_discipline_action_length, da.discipline_action_length),0) > 0
    group by da.school_year, da.tenant_code, da.k_student, da.k_school__responsibility, ssa.k_school_calendar,
        da.discipline_date, da.discipline_action_id,
        coalesce(da.actual_discipline_action_length, da.discipline_action_length)
),
disciplinary_windows as (
    select ds.school_year, ds.tenant_code, ds.k_student, ds.k_school__responsibility, ds.discipline_date, 
        ds.discipline_action_id, 
        ds.discipline_action_length, ds.first_possible_discipinary_date, ds.first_possible_discipinary_day,
        max(end_cd.calendar_date) as last_possible_discipinary_date,
        max(end_cd.day_of_school_year) as last_possible_discipinary_day
    from disciplinary_start_dates ds
    left outer join {{ ref('dim_calendar_date') }} end_cd
        on end_cd.school_year = ds.school_year
        and end_cd.tenant_code = ds.tenant_code
        and end_cd.k_school_calendar = ds.k_school_calendar
        and end_cd.is_school_day is true
        and end_cd.day_of_school_year >= ds.first_possible_discipinary_day
        and end_cd.day_of_school_year <=
            ((ds.first_possible_discipinary_day-1) + ds.discipline_action_length)
    group by ds.school_year, ds.tenant_code, ds.k_student, ds.k_school__responsibility, ds.discipline_date, 
        ds.discipline_action_id,
        ds.discipline_action_length, ds.first_possible_discipinary_date, ds.first_possible_discipinary_day
)
select school_year, tenant_code, k_student, discipline_date, discipline_action_id,
    last_possible_discipinary_date as discipline_end_date
from disciplinary_windows