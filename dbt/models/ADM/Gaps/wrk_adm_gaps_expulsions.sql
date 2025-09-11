{{
  config(
    materialized="table",
    schema="stg_adm_gaps"
  )
}}

with q as (
    select e.school_year, e.k_student, e.k_school, e.is_primary_school,
        ex.discipline_date_begin, ex.discipline_date_end,
        sum(
            case
                when ex.discipline_date is not null then 1
                else 0
            end
        ) as expelled_days
    from {{ ref('adm_gaps_enrollments') }} e
    join {{ ref('stg_ef3__calendar_dates') }} dates
        on dates.school_year = e.school_year
        and dates.k_school_calendar = e.k_school_calendar
        and dates.v_calendar_events LIKE '%uri://tdoe.edu/CalendarEventDescriptor#ID%'
        and dates.calendar_date >= e.entry_date
        and (e.exit_withdraw_date is null 
            or dates.calendar_date < e.exit_withdraw_date)
    join {{ ref('wrk_expulsion_windows') }} ex
        on e.k_student = ex.k_student
        and e.k_school = ex.k_school
        and e.school_year = ex.school_year
        and dates.calendar_date between ex.discipline_date_begin and ex.discipline_date_end
    group by e.school_year, e.k_student, e.k_school, e.is_primary_school, ex.discipline_date_begin, 
        ex.discipline_date_end
)
select school_year, k_student, k_school, is_primary_school,
    'funding' as reason_type,
    1 as reason_count,
    concat('Student is expelled for ', expelled_days,' days (', 
        discipline_date_begin, ' - ', discipline_date_end, ')') as possible_reason
from q