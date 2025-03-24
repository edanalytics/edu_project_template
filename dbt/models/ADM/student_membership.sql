{{
  config(
    materialized="table",
    schema="stg_adm"
  )
}}

with q as (
    select fssa.k_student, fssa.k_lea, fssa.k_school, fssa.k_school_calendar,
        fssa.school_year, fssa.is_primary_school, fssa.entry_date, fssa.exit_withdraw_date,
        gl.grade_level_short as grade_level, gl.grade_level_adm,
        case
            when fssa.exit_withdraw_type = 'Early Graduate' then 1
            else 0
        end as is_early_graduate, 
        ssd.ssd_duration,
        dcd.calendar_date, dcd.day_of_school_year, 
        dcd.report_period, dcd.report_period_begin_date, dcd.report_period_end_date,
        dcd.days_in_report_period,
        case
            when exists(
                select 1
                from {{ ref('fct_student_characteristics') }} x 
                where x.k_student = fssa.k_student 
                    and x.k_lea = fssa.k_lea
                    and x.student_characteristic IN ('FundineligI20', 'FundineligOOS')
                    and dcd.calendar_date >= x.begin_date 
                    and (x.end_date is null or dcd.calendar_date <= x.end_date)
                ) then 1
            else 0
        end as is_funding_ineligible,
        case
            when exists(
                select 1
                from {{ ref('wrk_expulsion_windows') }} x
                where x.k_student = fssa.k_student
                    and x.k_school = fssa.k_school
                    and dcd.calendar_date between x.discipline_date_begin and x.discipline_date_end
            ) then 1
            else 0
        end as is_expelled,
        case
            when exists(
                select 1
                from {{ ref('fct_student_characteristics') }} x
                where x.k_student = fssa.k_student
                    and x.k_lea = fssa.k_lea
                    and x.student_characteristic in ('I', 'J', 'H', 'U', 'FOS01')
                    and dcd.calendar_date >= x.begin_date 
                    and (x.end_date is null or dcd.calendar_date <= x.end_date)
            ) then 1
            else 0
        end as is_EconDis
    from {{ ref('fct_student_school_association') }} fssa
    join {{ ref('xwalk_grade_levels') }} gl
        on upper(gl.grade_level) = upper(fssa.entry_grade_level)
    join {{ ref('dim_calendar_date') }} dcd
        on dcd.k_school_calendar = fssa.k_school_calendar
        and dcd.is_school_day = true
    left outer join {{ ref('fct_student_standard_day') }} ssd
        on ssd.k_school = fssa.k_school
        and ssd.k_student = fssa.k_student
        and dcd.calendar_date between ssd.ssd_date_start and ssd.ssd_date_end
    where fssa.school_year = 2025
)
select k_student, k_lea, k_school, k_school_calendar,
    school_year, is_primary_school, entry_date, exit_withdraw_date,
    grade_level, grade_level_adm, coalesce(is_early_graduate,0) as is_early_graduate, ssd_duration,
    calendar_date, day_of_school_year, report_period, report_period_begin_date,
    report_period_end_date, days_in_report_period, 
    is_funding_ineligible, is_expelled, is_EconDis,
    case
        when exit_withdraw_date is not null and calendar_date >= exit_withdraw_date 
            and is_early_graduate = 1 then 1
        else 0
    end as is_early_grad_date,
    case
        when is_expelled = 1 /*todo: need is_sped here */ then 0 
        when is_funding_ineligible = 1 then 0
        when calendar_date >= entry_date 
            and is_early_graduate = 1 then 1
        when calendar_date >= entry_date
            and (exit_withdraw_date is null
                or calendar_date <= exit_withdraw_date) then 1
        else 0
    end as has_membership
from q
order by k_school, k_student, calendar_date