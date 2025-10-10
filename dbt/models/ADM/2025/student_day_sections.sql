{{
  config(
    materialized="table",
    schema="stg_adm"
  )
}}

/* 
The purpose of this model is to get a Student's Classes by Day for ADM.
*/

with student_classes as (
    /* Get the Student Classes for Student Days for when the Day is not an Early Grad Date. 
    Early Grads Dates don't have classes, so we'll take care of them later. */
    select sm.k_student, sm.k_lea, sm.k_school, sm.k_school_calendar, sm.school_year,
        sm.is_primary_school, sm.entry_date, sm.exit_withdraw_date, sm.grade_level, sm.grade_level_adm,
        sm.is_early_graduate, sm.calendar_date, sm.isa_member,
        is_sped, is_funding_ineligible, is_expelled, is_EconDis, is_EL, is_Dyslexic,
        sm.is_early_grad_date,
        sm.ssd_duration,
        sm.report_period, sm.report_period_begin_date, sm.report_period_end_date,
        sm.days_in_report_period,
        si.course_code, si.period_duration,
        case
            when si.is_cte then 1
            else 0
        end as is_vocational_course
    from {{ ref('student_days') }} sm
    join {{ ref('fct_student_section_association') }} fssa
        on fssa.school_year = sm.school_year
        and fssa.k_student = sm.k_student
        and fssa.k_school = sm.k_school
        /* The calendar date must be between the student's section dates. */
        and sm.calendar_date >= fssa.begin_date 
        and (fssa.end_date is null or sm.calendar_date <= fssa.end_date)
    join {{ ref('course_section_days') }} si
        on si.school_year = fssa.school_year
        and si.k_course_section = fssa.k_course_section
        and si.k_school = fssa.k_school
        and si.calendar_date = sm.calendar_date
    where sm.is_early_grad_date = 0
),
aggregated_courses as (
    /* Need to get the period duration for non-voc courses. The reason is because voc courses
    are used in Voc ADM. So we will get their durations separately. */
    select k_student, k_lea, k_school, k_school_calendar, school_year,
        is_primary_school, entry_date, exit_withdraw_date, grade_level, grade_level_adm,
        is_early_graduate, calendar_date, isa_member,
        is_sped, is_funding_ineligible, is_expelled, is_EconDis, is_EL, is_Dyslexic,
        is_early_grad_date,
        ssd_duration,
        report_period, report_period_begin_date, report_period_end_date,
        days_in_report_period,
        concat_ws(', ', collect_list(course_code)) as course_code, 
        sum(coalesce(period_duration,0)) as period_duration,
        is_vocational_course
    from student_classes
    where is_vocational_course = 0
    group by k_student, k_lea, k_school, k_school_calendar, school_year,
        is_primary_school, entry_date, exit_withdraw_date, grade_level, grade_level_adm,
        is_early_graduate, calendar_date, isa_member,
        is_sped, is_funding_ineligible, is_expelled, is_EconDis, is_EL, is_Dyslexic,
        is_early_grad_date,
        ssd_duration,
        report_period, report_period_begin_date, report_period_end_date,
        days_in_report_period, is_vocational_course
),
non_aggregated_courses as (
    /* Voc courses have their periods here so we can easily do the voc adm later. */
    select k_student, k_lea, k_school, k_school_calendar, school_year,
        is_primary_school, entry_date, exit_withdraw_date, grade_level, grade_level_adm,
        is_early_graduate, calendar_date, isa_member,
        is_sped, is_funding_ineligible, is_expelled, is_EconDis, is_EL, is_Dyslexic,
        is_early_grad_date,
        ssd_duration,
        report_period, report_period_begin_date, report_period_end_date,
        days_in_report_period,
        course_code, 
        coalesce(period_duration,0) as period_duration,
        is_vocational_course
    from student_classes
    where is_vocational_course = 1
)
/* Here's the Early Grad Dates. They don't have classes, so they don't have period durations. */
select sm.k_student, sm.k_lea, sm.k_school, sm.k_school_calendar, sm.school_year,
    sm.is_primary_school, sm.entry_date, sm.exit_withdraw_date, sm.grade_level, sm.grade_level_adm,
    sm.is_early_graduate, sm.calendar_date, sm.isa_member,
    is_sped, is_funding_ineligible, is_expelled, is_EconDis, is_EL, is_Dyslexic,
    sm.is_early_grad_date,
    sm.ssd_duration,
    sm.report_period, sm.report_period_begin_date, sm.report_period_end_date,
    sm.days_in_report_period,
    null as course_code, null as period_duration, null as is_vocational_course
from {{ ref('student_days') }} sm
where sm.is_early_grad_date = 1
union all
/* Here's the rest of the Dates, which have classes and period durations. */
select *
from aggregated_courses
union all
select *
from non_aggregated_courses