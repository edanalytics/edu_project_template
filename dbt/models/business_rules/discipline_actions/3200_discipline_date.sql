{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3200 %}

with stg_discipline_actions as (
    select * from {{ ref('stg_ef3__discipline_actions_orig') }}
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
/* Disipline Date must fall on an Instructional Day. */
select da.k_student, da.k_school__responsibility, da.school_year,
    da.discipline_action_id, da.discipline_date, da.responsibility_school_id,
    da.student_unique_id,
    {{ error_code }} as error_code,
    concat('Discipline Date does not fall on an Instructional Day of the School Calendar used by this Student. Discipline Date: ',
        da.discipline_date, '.') as error,
    {{ error_severity_column(error_code, 'da') }}
from discipline_start_day_of_school_year da
where da.day_of_school_year is not null