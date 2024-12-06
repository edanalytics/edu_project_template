{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3201 %}

with stg_discipline_actions as (
    select * 
    from {{ ref('stg_ef3__discipline_actions_orig') }}
    where discipline_action_length is not null
),
calendar_dates as (
    select k_school, calendar_code, school_year, calendar_date, day_of_school_year
    from {{ ref('dim_calendar_date') }}
    where is_school_day = true
),
discipline_start_day_of_school_year as (
    select da.k_student, da.k_school__responsibility, da.school_year,
        da.discipline_action_id, da.discipline_date, da.responsibility_school_id,
        da.student_unique_id,
        ceiling(da.discipline_action_length) as discipline_action_length,
        ssa.calendar_code,
        cd.day_of_school_year
    from stg_discipline_actions da
    join {{ ref('stg_ef3__student_school_associations_orig') }} ssa
        on ssa.k_student = da.k_student
        and ssa.k_school = da.k_school__responsibility
    left outer join calendar_dates cd
        on cd.k_school = da.k_school__responsibility
        and cd.school_year = da.school_year
        and cd.calendar_code = ssa.calendar_code
        and cd.calendar_date = da.discipline_date
)
/* Discipline End Date must be an Instructional Day. */
select da.k_student, da.k_school__responsibility, da.school_year,
    da.discipline_action_id, da.discipline_date, da.responsibility_school_id,
    da.student_unique_id,
    {{ error_code }} as error_code,
    concat('Discipline Action Length produces an end date that does not fall on an Instructional Day of the School Calendar used by this Student. Discipline Date: ',
        da.discipline_date, ', Discipline Action Length: ', da.discipline_action_length, '.') as error,
    {{ error_severity_column(error_code, 'da') }}
from discipline_start_day_of_school_year da
where not exists (
        select 1
        from calendar_dates cd
        where cd.k_school = da.k_school__responsibility
            and cd.school_year = da.school_year
            and cd.calendar_code = da.calendar_code
            and cd.day_of_school_year = ((da.day_of_school_year-1) + da.discipline_action_length)
    )