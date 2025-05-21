{{
  config(
    materialized="table",
    schema="stg_adm"
  )
}}

select k_student, k_lea, k_school, k_school_calendar, school_year, is_primary_school, entry_date,
    exit_withdraw_date, grade_level, grade_level_adm, is_early_graduate, 
    calendar_date, isa_member, is_funding_ineligible, is_expelled, is_EconDis,
    is_early_grad_date,
    ssd_duration, report_period, report_period_begin_date, report_period_end_date,
    days_in_report_period,
    sum(
        case
            when period_duration is null then 0
            when isa_member = 1 then period_duration
            else 0
        end
    ) as class_duration,
    cast(
        (floor(
            (case
                when is_early_grad_date = 1 then 1
                when ssd_duration is null or ssd_duration = 0 then 0
                when sum(period_duration) is null then 0
                when isa_member = 1 then
                    cast(sum(period_duration) as decimal(12,8)) / cast(ssd_duration as decimal(12,8))
                else 0
            end) * 100000.0) / 100000.0)
        as decimal(8,5)
    ) as membership,
    cast(
        (floor(
            (case
                when coalesce(is_EconDis,0) = 0 then 0
                when is_early_grad_date = 1 then 1
                when ssd_duration is null or ssd_duration = 0 then 0
                when sum(period_duration) is null then 0
                when isa_member = 1 then
                    cast(sum(period_duration) as decimal(12,8)) / cast(ssd_duration as decimal(12,8))
                else 0
            end) * 100000.0) / 100000.0)
        as decimal(8,5)
    ) as ed_membership,
    max(is_vocational_course) as has_vocational_courses
from {{ ref('student_day_sections') }}
group by k_student, k_lea, k_school, k_school_calendar, school_year, is_primary_school, entry_date,
    exit_withdraw_date, grade_level, grade_level_adm, is_early_graduate, 
    calendar_date, isa_member, is_funding_ineligible, is_expelled, is_EconDis,
    is_early_grad_date,
    ssd_duration, report_period, report_period_begin_date, report_period_end_date,
    days_in_report_period