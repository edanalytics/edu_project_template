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

with student_classes as (
    select sm.k_student, sm.k_lea, sm.k_school, sm.k_school_calendar, sm.school_year,
        sm.is_primary_school, sm.entry_date, sm.exit_withdraw_date, sm.grade_level, sm.grade_level_adm,
        sm.is_early_graduate, sm.calendar_date, sm.has_membership,
        coalesce(sm.is_funding_ineligible, 0) as is_funding_ineligible, 
        coalesce(sm.is_expelled,0) as is_expelled, coalesce(sm.is_EconDis,0) as is_EconDis, 
        sm.is_early_grad_date,
        sm.ssd_duration,
        sm.report_period, sm.report_period_begin_date, sm.report_period_end_date,
        sm.days_in_report_period,
        sec.k_course_section, sec.begin_date, sec.end_date,
        scpd.k_course_section, scpd.course_code, scpd.k_class_period, scpd.period_duration,
        case
            when scpd.is_cte = 1 then 1
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
),
student_membership_by_day as (
    select k_student, k_lea, k_school, k_school_calendar, school_year, is_primary_school, entry_date,
        exit_withdraw_date, grade_level, grade_level_adm, is_early_graduate, 
        calendar_date, has_membership, is_funding_ineligible, is_expelled, is_EconDis,
        is_early_grad_date,
        ssd_duration, report_period, report_period_begin_date, report_period_end_date,
        days_in_report_period,
        sum(
            case
                when period_duration is null then 0
                when has_membership = 1 then period_duration
                else 0
            end
        ) as class_duration,
        cast(
            (floor(
                (case
                    when is_early_grad_date = 1 then 1
                    when ssd_duration is null or ssd_duration = 0 then 0
                    when sum(period_duration) is null then 0
                    when has_membership = 1 then
                        cast(sum(period_duration) as decimal(12,8)) / cast(ssd_duration as decimal(12,8))
                    else 0
                end) * 100000.0) / 100000.0)
            as decimal(8,5)
        ) as membership,
        max(is_vocational_course) as has_vocational_courses
    from student_classes
    group by k_student, k_lea, k_school, k_school_calendar, school_year, is_primary_school, entry_date,
        exit_withdraw_date, grade_level, grade_level_adm, is_early_graduate, 
        calendar_date, has_membership, is_funding_ineligible, is_expelled, is_EconDis,
        is_early_grad_date,
        ssd_duration, report_period, report_period_begin_date, report_period_end_date,
        days_in_report_period
)
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
    sum(sm.is_EconDis) as days_EconDis,
    sum(sm.membership) as sum_membership,
    sum(sm.ssd_duration) as sum_student_standard_day,
    sum(sm.class_duration) as sum_class_duration,
    cast(
        (floor(
            (case
                when sm.days_in_report_period is null or sm.days_in_report_period = 0 then 0
                when sum(sm.membership) is null or sum(sm.membership) = 0 then 0
                else sum(sm.membership) / cast(sm.days_in_report_period as decimal(12,8))
            end) * 100000) / 100000)
        as decimal(8,5)
    ) as actual_adm,
    cast(
        (floor(
            (case
                when sm.days_in_report_period is null or sm.days_in_report_period = 0 then 0
                when sum(sm.membership) is null or sum(sm.membership) = 0 then 0
                else least(
                        sum(least(sm.membership, 1.0)) / 
                            cast(least(sm.days_in_report_period,20) as decimal(12,8)), 1.0)
            end) * 100000) / 100000)
        as decimal(8,5)
    ) as normalized_adm
from student_membership_by_day sm
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
    sm.report_period, sm.report_period_begin_date, sm.report_period_end_date, sm.days_in_report_period
order by sm.k_lea, sm.k_school, sm.k_student, sm.report_period