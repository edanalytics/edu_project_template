{{
  config(
    materialized="table",
    schema="stg_adm_gaps"
  )
}}

with bad_cals as (
    select school_year, k_school, k_school_calendar, 
        count(*) as reason_count,
        concat_ws('\n', collect_list(concat('\t', error))) as errors
    from {{ ref('calendars') }} x
    where severity = 'critical'
    group by school_year, k_school, k_school_calendar
)
select school_year, null as k_student, k_school, null as is_primary_school,
    'calendar' as reason_type,
    reason_count,
    concat('School has the following Calendar errors:\n', errors) as possible_reason
from bad_cals
union all
select e.school_year, e.k_student, e.k_school, e.is_primary_school,
    'calendar' as reason_type,
    reason_count,
    concat('Student tied to calendar with the following errors:\n', x.errors) as possible_reason
from {{ ref('adm_gaps_enrollments') }} e
join bad_cals x
    on x.school_year = e.school_year
    and x.k_school = e.k_school
    and x.k_school_calendar = e.k_school_calendar