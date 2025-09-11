{{
  config(
    materialized="table",
    schema="stg_adm_gaps"
  )
}}

with q as (
    select e.school_year, e.k_student, e.k_school, e.is_primary_school,
        sum(
            case
                when c.begin_date is not null then 1
                else 0
            end
        ) as ineligible_days
    from {{ ref('adm_gaps_enrollments') }} e
    join {{ ref('stg_ef3__calendar_dates') }} dates
        on dates.school_year = e.school_year
        and dates.k_school_calendar = e.k_school_calendar
        and dates.v_calendar_events LIKE '%uri://tdoe.edu/CalendarEventDescriptor#ID%'
        and dates.calendar_date >= e.entry_date
        and (e.exit_withdraw_date is null 
            or dates.calendar_date < e.exit_withdraw_date)
    join (
            select c.k_student, s.k_school, c.begin_date, c.end_date
            from {{ ref('stg_ef3__stu_ed_org__characteristics') }} c
            join {{ ref('stg_ef3__schools') }} s
                on s.k_lea = c.k_lea
            where c.k_lea is not null
                and c.student_characteristic IN ('FundineligI20', 'FundineligOOS')
                and c.begin_date is not null
        ) c
        on e.k_student = c.k_student
        and e.k_school = c.k_school
        and dates.calendar_date >= c.begin_date
        and (c.end_date is null or dates.calendar_date <= c.end_date)
    group by e.school_year, e.k_student, e.k_school, e.is_primary_school
)
select school_year, k_student, k_school, is_primary_school,
    'funding' as reason_type,
    1 as reason_count,
    concat('Student is funding ineligible for ', ineligible_days,' days.') as possible_reason
from q