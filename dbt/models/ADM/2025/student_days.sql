{{
  config(
    materialized="table",
    schema="stg_adm"
  )
}}

/* 
The purpose of this model is to get every student by day and all their various flags for that day.
This is needed to determine the membership calculation, so the main column here is the "isAMember" column.
*/

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
            when sped.participation_status is not null then 1
            else 0
        end as is_SPED,
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
                    and x.student_characteristic in ('I', 'J', 'H', 'U', 'FOS01', 'SN', 'TO')
                    and dcd.calendar_date >= x.begin_date 
                    and (x.end_date is null or dcd.calendar_date <= x.end_date)
            ) then 1
            else 0
        end as is_EconDis,
        case
            when ilp.participation_status is not null then 1
            else 0
        end as is_EL,
        case
            when ilpd.service_begin_date is not null then 1
            else 0
        end as is_Dyslexic
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
    left outer join {{ ref('bld_sped_safe_ranges') }} sped
        on sped.k_lea = fssa.k_lea
        and sped.k_student = fssa.k_student
        and sped.tenant_code = fssa.tenant_code
        and sped.school_year = fssa.school_year
        and sped.primary_indicator = true
        and dcd.calendar_date between sped.service_begin_date and sped.safe_service_end_date
    left outer join {{ ref('bld_ilp_safe_ranges') }} ilp
        on ilp.k_school = fssa.k_school
        and ilp.k_student = fssa.k_student
        and ilp.school_year = fssa.school_year
        and (
                (ilp.seq = 1
                    and dcd.calendar_date between 
                        (case
                            when datediff(ilp.status_begin_date, fssa.entry_date) > 0 and datediff(ilp.status_begin_date, fssa.entry_date) <= 60 then fssa.entry_date
                            else ilp.status_begin_date
                        end)
                        and ilp.safe_status_end_date
                ) or (
                    ilp.seq != 1
                    and dcd.calendar_date between ilp.status_begin_date and ilp.safe_status_end_date
                )
            )
    left outer join {{ ref('bld_ilpd_safe_ranges') }} ilpd
        on ilpd.k_school = fssa.k_school
        and ilpd.k_student = fssa.k_student
        and dcd.calendar_date between ilpd.service_begin_date and ilpd.safe_service_end_date
)
select k_student, k_lea, k_school, k_school_calendar,
    school_year, is_primary_school, entry_date, exit_withdraw_date,
    grade_level, grade_level_adm, coalesce(is_early_graduate,0) as is_early_graduate, ssd_duration,
    calendar_date, day_of_school_year, report_period, report_period_begin_date,
    report_period_end_date, days_in_report_period, 
    coalesce(is_sped,0) as is_sped,
    coalesce(is_funding_ineligible,0) as is_funding_ineligible,
    coalesce(is_expelled,0) as is_expelled, 
    coalesce(is_EconDis,0) as is_EconDis,
    coalesce(is_EL,0) as is_EL,
    coalesce(is_Dyslexic,0) as is_Dyslexic,
    case
        when exit_withdraw_date is not null and calendar_date >= exit_withdraw_date 
            and is_early_graduate = 1 then 1
        else 0
    end as is_early_grad_date,
    case
        when is_expelled = 1 and coalesce(is_sped,0) = 0 then 0 
        when is_funding_ineligible = 1 then 0
        when calendar_date >= exit_withdraw_date 
            and is_early_graduate = 1 then 1
        when calendar_date >= entry_date
            and (exit_withdraw_date is null
                or calendar_date < exit_withdraw_date) then 1
        else 0
    end as isa_member
from q
order by k_school, k_student, calendar_date