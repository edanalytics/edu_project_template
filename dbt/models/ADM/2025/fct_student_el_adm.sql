{{
  config(
    materialized="table",
    schema="wh",
    post_hook=[ 
        "alter table {{ this }} alter column k_student set not null",
        "alter table {{ this }} alter column k_lea set not null",
        "alter table {{ this }} alter column k_school set not null",
        "alter table {{ this }} alter column k_school_calendar set not null",
        "alter table {{ this }} alter column report_period set not null",
        "alter table {{ this }} add primary key (k_student, k_lea, k_school, k_school_calendar, report_period)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('edu_wh', 'dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_lea foreign key (k_lea) references {{ ref('edu_wh', 'dim_lea') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('edu_wh', 'dim_school') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school_calendar foreign key (k_school_calendar) references {{ ref('edu_wh', 'dim_school_calendar') }}"
    ]
  )
}}

/* This model calculates the English Language Learners ADM. 
I don't know why this is a thing. */

with raw_el_adm as (
    select sm.k_student, sm.k_lea, sm.k_school, sm.k_school_calendar, sm.school_year, 
        l.lea_id as district_id, l.lea_name as district_name, 
        cast(right(cast(school.school_id as string), 4) as int) as school_id, school.school_name,
        s.student_unique_id,
        sm.is_primary_school, sm.entry_date,
        sm.exit_withdraw_date, sm.grade_level, sm.grade_level_adm, sm.is_early_graduate, 
        sm.report_period, sm.report_period_begin_date, sm.report_period_end_date, sm.days_in_report_period,
        max(sm.has_vocational_courses) as has_vocational_courses,
        sum(sm.is_funding_ineligible) as days_funding_ineligible,
        sum(sm.is_expelled) as days_expelled,
        -1 as days_sped,
        sum(sm.is_EL) as days_EL,
        sum(sm.el_membership) as sum_el_membership,
        sum(sm.ssd_duration) as sum_student_standard_day,
        sum(sm.class_duration) as sum_class_duration,
        cast(
            (floor(
                (case
                    when sm.days_in_report_period is null or sm.days_in_report_period = 0 then 0
                    when sum(sm.el_membership) is null or sum(sm.el_membership) = 0 then 0
                    else sum(sm.el_membership) / cast(sm.days_in_report_period as decimal(12,8))
                end) * 100000) / 100000)
            as decimal(8,5)
        ) as actual_el_adm,
        cast(
            (floor(
                (case
                    when sm.days_in_report_period is null or sm.days_in_report_period = 0 then 0
                    when sum(sm.el_membership) is null or sum(sm.el_membership) = 0 then 0
                    else least(
                            sum(least(sm.el_membership, 1.0)) / 
                                cast(least(sm.days_in_report_period,20) as decimal(12,8)), 1.0)
                end) * 100000) / 100000)
            as decimal(8,5)
        ) as normalized_el_adm,
        ilp.participation_status,
        ilp.status_begin_date as calc_status_begin_date,
        ilp.safe_status_end_date as calc_status_end_date,
        ilp.total_years_esl,
        case
            when ilp.seq = 1
                    and sm.calendar_date between 
                        (case
                            when datediff(ilp.status_begin_date, sm.entry_date) > 0 and datediff(ilp.status_begin_date, sm.entry_date) <= 60 then sm.entry_date
                            else ilp.status_begin_date
                        end)
                        and ilp.safe_status_end_date then true
            else false
        end as is_generous
    from {{ ref('student_membership') }} sm
    left outer join {{ ref('bld_ilp_safe_ranges') }} ilp
        on ilp.k_school = sm.k_school
        and ilp.k_student = sm.k_student
        and ilp.school_year = sm.school_year
        and (
                (ilp.seq = 1
                    and sm.calendar_date between 
                        (case
                            when datediff(ilp.status_begin_date, sm.entry_date) > 0 and datediff(ilp.status_begin_date, sm.entry_date) <= 60 then sm.entry_date
                            else ilp.status_begin_date
                        end)
                        and ilp.safe_status_end_date
                ) or (
                    ilp.seq != 1
                    and sm.calendar_date between ilp.status_begin_date and ilp.safe_status_end_date
                )
            )
    join {{ ref('dim_student') }} s
        on s.k_student = sm.k_student
    join {{ ref('dim_lea') }} l
        on l.k_lea = sm.k_lea
    join {{ ref('dim_school') }} school
        on school.k_school = sm.k_school
    group by sm.k_student, sm.k_lea, sm.k_school, sm.k_school_calendar, sm.school_year,  
        l.lea_id, l.lea_name, cast(right(cast(school.school_id as string), 4) as int), school.school_name,
        s.student_unique_id,
        sm.is_primary_school, sm.entry_date,
        sm.exit_withdraw_date, sm.grade_level, sm.grade_level_adm, sm.is_early_graduate, 
        sm.report_period, sm.report_period_begin_date, sm.report_period_end_date, sm.days_in_report_period,
        ilp.participation_status,
        ilp.status_begin_date,
        ilp.safe_status_end_date,
        ilp.total_years_esl,
        case
            when ilp.seq = 1
                    and sm.calendar_date between 
                        (case
                            when datediff(ilp.status_begin_date, sm.entry_date) > 0 and datediff(ilp.status_begin_date, sm.entry_date) <= 60 then sm.entry_date
                            else ilp.status_begin_date
                        end)
                        and ilp.safe_status_end_date then true
            else false
        end
)
select x.*
from raw_el_adm x
order by x.k_lea, x.k_school, x.k_student, x.report_period