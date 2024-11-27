{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 2006 %}

with stg_sections as (
    select * from {{ ref('stg_ef3__sections_orig') }}
),
stg_sections_class_periods as (
    select s.k_course_section, s.educational_environment_type, cp.*
    from stg_sections s
    join {{ ref('stg_ef3__sections__class_periods') }} scp
        on scp.k_course_section = s.k_course_section
    join {{ ref('stg_ef3__class_periods') }} cp
        on cp.k_class_period = scp.k_class_period
),
nonPullOutsMissingClassPeriodDurations as (
    select k_course_section,
        cast(array_agg(class_period_name) as String) as class_periods
    from (
        select cp.k_course_section, 
            cp.class_period_name,
            v_meeting_times:[0].startTime::timestamp as start_time,
            v_meeting_times:[0].endTime::timestamp as end_time,
            timediff(MINUTE, start_time, end_time) as period_duration
        from stg_sections_class_periods cp
        where ifnull(cp.educational_environment_type,'X') != 'P'
    ) x
    where ifnull(period_duration, 0) <= 0
    group by k_course_section
)
/* Sections that are not Pull Out must have a meeting time duration. */
select s.k_course_section, s.k_course_offering, s.k_school, s.k_location, s.k_school__location, 
    s.section_id, s.local_course_code, s.school_id, s.school_year, s.session_name,
    {{ error_code }} as error_code,
    concat('Sections with an Educational Environment Descriptor of "', ifnull(s.educational_environment_type, 'null'), 
        '" must submit Class Periods with valid durations. Class Periods with invalid durations: ', x.class_periods) as error,
    {{ error_severity_column(error_code, 's') }}
from stg_sections s
join nonPullOutsMissingClassPeriodDurations x
    on x.k_course_section = s.k_course_section