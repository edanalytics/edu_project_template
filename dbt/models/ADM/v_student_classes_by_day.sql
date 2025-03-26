{{
  config(
    materialized="view",
    schema="stg_adm"
  )
}}

select sm.k_student, sm.k_lea, sm.k_school, sm.k_school_calendar, sm.school_year,
    sm.is_primary_school, sm.entry_date, sm.exit_withdraw_date, sm.grade_level, sm.grade_level_adm,
    sm.is_early_graduate, sm.calendar_date, sm.isa_member,
    coalesce(sm.is_funding_ineligible, 0) as is_funding_ineligible, 
    coalesce(sm.is_expelled,0) as is_expelled, coalesce(sm.is_EconDis,0) as is_EconDis, 
    sm.is_early_grad_date,
    sm.ssd_duration,
    sm.report_period, sm.report_period_begin_date, sm.report_period_end_date,
    sm.days_in_report_period,
    sec.k_course_section, sec.begin_date, sec.end_date,
    scpd.course_code, scpd.k_class_period, scpd.period_duration,
    case
        when scpd.is_cte then 1
        else 0
    end as is_vocational_course
from {{ ref('student_membership') }} sm
left outer join {{ ref('edu_wh', 'fct_student_section_association') }} sec
    on sec.k_student = sm.k_student
    and sec.k_school = sm.k_school
    /* The calendar date must be between the student's section dates. */
    and sm.calendar_date >= sec.begin_date 
    and (sec.end_date is null 
        or sm.calendar_date <= sec.end_date)
left outer join (
        select k_course_section, k_class_period, calendar_date, 
            course_code, period_duration, is_cte
        from {{ ref('fct_section_class_period_dates') }}
        where ifnull(educational_environment_type,'X') != 'P' /* Remove pull out classes. */
            and course_code not in ('G25H09','G25X23') /* Remove cafeteria courses */
    ) scpd
    on scpd.k_course_section = sec.k_course_section
    and scpd.calendar_date = sm.calendar_date