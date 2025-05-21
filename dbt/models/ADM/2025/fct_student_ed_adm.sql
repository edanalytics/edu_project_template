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

with raw_ed_adm as (
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
        sum(sm.ed_membership) as sum_ed_membership,
        sum(sm.ssd_duration) as sum_student_standard_day,
        sum(sm.class_duration) as sum_class_duration,
        cast(
            (floor(
                (case
                    when sm.days_in_report_period is null or sm.days_in_report_period = 0 then 0
                    when sum(sm.ed_membership) is null or sum(sm.ed_membership) = 0 then 0
                    else sum(sm.ed_membership) / cast(sm.days_in_report_period as decimal(12,8))
                end) * 100000) / 100000)
            as decimal(8,5)
        ) as actual_ed_adm,
        cast(
            (floor(
                (case
                    when sm.days_in_report_period is null or sm.days_in_report_period = 0 then 0
                    when sum(sm.ed_membership) is null or sum(sm.ed_membership) = 0 then 0
                    else least(
                            sum(least(sm.ed_membership, 1.0)) / 
                                cast(least(sm.days_in_report_period,20) as decimal(12,8)), 1.0)
                end) * 100000) / 100000)
            as decimal(8,5)
        ) as normalized_ed_adm
    from {{ ref('student_membership') }} sm
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
),
ed_ranges as (
    select tenant_code, k_student, k_lea, 
        student_characteristic, begin_date, coalesce(end_date, cast('9999-12-31' as date)) as end_date,
        concat(student_characteristic, 
            '(', begin_date, ' - ', 
            coalesce(end_date,'null'), ')'
        ) as sc_range
    from {{ ref('fct_student_characteristics') }}
    where student_characteristic in ('I', 'J', 'H', 'U', 'FOS01')
),
contributing_eds as (
    select k_student, k_lea, school_year, report_period,
        concat_ws(', ', collect_list(sc_range)) as contributing_eds
    from (
        select distinct x.k_student, x.k_lea, x.school_year, x.report_period, er.sc_range
        from raw_ed_adm x
        join ed_ranges er
        where er.k_student = x.k_student
            and er.k_lea = x.k_lea
            and er.begin_date <= x.report_period_end_date
            and er.end_date >= x.report_period_begin_date
    ) 
    group by k_student, k_lea, school_year, report_period
)
select x.*,
    y.contributing_eds
from raw_ed_adm x
left outer join contributing_eds y
    on y.school_year = x.school_year
    and y.k_student = x.k_student
    and y.k_lea = x.k_lea
    and y.report_period = x.report_period
order by x.k_lea, x.k_school, x.k_student, x.report_period