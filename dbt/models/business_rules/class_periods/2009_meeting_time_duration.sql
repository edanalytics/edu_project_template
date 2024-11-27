{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 2009 %}

with stg_class_periods as (
    select * from {{ ref('stg_ef3__class_periods_orig') }}
),
tooManyClassPeriods as (
    select cp.k_class_period,
        v_meeting_times,
        size(cast(v_meeting_times as array<string>)) meetingTimesCount
    from stg_class_periods cp
    where size(cast(v_meeting_times as array<string>)) > 1
),
invalidDurations as (
    select k_class_period, period_duration
    from (
        select cp.k_class_period,
            v_meeting_times:[0].startTime::timestamp as start_time,
            v_meeting_times:[0].endTime::timestamp as end_time,
            timediff(MINUTE, start_time, end_time) as period_duration
        from stg_class_periods cp
        where not exists (
            select 1
            from tooManyClassPeriods x
            where x.k_class_period = cp.k_class_period
        )
    ) x
    where period_duration is not null
        and period_duration < 0
)
/* Class Periods must have a positive meeting time duration. */
select cp.k_class_period, cp.class_period_name, cp.school_id,
    {{ error_code }} as error_code,
    concat('Class Period has a negative meeting duration. Please use military time. Meeting Time: ', cast(cp.v_meeting_times as String)) as error,
    {{ error_severity_column(error_code, 'cp') }}
from stg_class_periods cp
join invalidDurations x
    on x.k_class_period = cp.k_class_period