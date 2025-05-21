{{
  config(
    materialized="table",
    schema="stg_adm"
  )
}}

/*
Student Expulsions has a start date and then a number of school days. But that's hard to reason out
when joining lots of other tables. So the purpose of this model is to figure out expulsion windows.
That is to say, a student is expelled from Date A to Date B, inclusive. That's much easier to use
when you need to know if they are expelled on any given day (for ADM calculations).
*/

select k_student, k_school, school_year, tenant_code, discipline_date, discipline_action_length,
    min(calendar_date) as discipline_date_begin,
    max(calendar_date) as discipline_date_end
from (
    select fsda.k_student, fsda.k_school, fsda.school_year, fsda.tenant_code, fsda.discipline_date, 
        coalesce(fsda.actual_discipline_action_length, fsda.discipline_action_length) as discipline_action_length,
        dcd.calendar_date,
        row_number() over (
            partition by fsda.k_student, fsda.k_school, fsda.school_year, fsda.tenant_code, 
                fsda.discipline_date
            order by dcd.calendar_date) as rn
    from {{ ref('fct_student_discipline_actions') }} fsda
    join {{ ref('fct_student_school_association') }} fssa
        on fssa.k_school = fsda.k_school
        and fssa.k_student = fsda.k_student
    join {{ ref('dim_calendar_date') }} dcd
        on dcd.k_school_calendar = fssa.k_school_calendar
        and dcd.k_school = fssa.k_school
        and dcd.is_school_day = true
    where fsda.discipline_action = 'E'
        and fsda.discipline_action_length is not null
        and coalesce(fsda.actual_discipline_action_length, fsda.discipline_action_length) > 0
        and dcd.calendar_date >= fsda.discipline_date
)
where rn <= discipline_action_length
group by k_student, k_school, school_year, tenant_code, discipline_date, discipline_action_length