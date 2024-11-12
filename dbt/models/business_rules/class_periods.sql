{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

with stg_class_periods as (
    select * from {{ ref('stg_ef3__class_periods') }}
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
/* Class Periods cannot have more than one meeting time. */
select cp.k_class_period, cp.class_period_name, cp.school_id,
    2007 as error_code,
    concat('Class Period has more than one meeting time. Meeting Times: ', cast(cp.v_meeting_times as String)) as error
from stg_class_periods cp
join tooManyClassPeriods x
    on x.k_class_period = cp.k_class_period
union
/* Class Periods must have a positive meeting time duration. */
select cp.k_class_period, cp.class_period_name, cp.school_id,
    2009 as error_code,
    concat('Class Period has a negative meeting duration. Please use military time. Meeting Time: ', cast(cp.v_meeting_times as String)) as error
from stg_class_periods cp
join invalidDurations x
    on x.k_class_period = cp.k_class_period