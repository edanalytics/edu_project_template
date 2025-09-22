{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 2009 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
stg_class_periods as (
    select * from {{ ref('stg_ef3__class_periods_orig') }} cp
    where exists (
        select 1
        from brule
        where cast(cp.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    )
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
select cp.k_class_period, cast(cp.school_year as int) as school_year, cp.class_period_name, cp.school_id,
    brule.tdoe_error_code as error_code,
    concat('Class Period ', cp.class_period_name, ' has a negative meeting duration. Please use military time. Meeting Time: ', cast(cp.v_meeting_times as String)) as error,
    brule.tdoe_severity as severity
from stg_class_periods cp
join invalidDurations x
    on x.k_class_period = cp.k_class_period
join brule
    on cp.school_year between brule.error_school_year_start and brule.error_school_year_end