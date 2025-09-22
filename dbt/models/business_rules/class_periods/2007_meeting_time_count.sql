{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 2007 %}

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
)
/* Class Periods cannot have more than one meeting time. */
select cp.k_class_period, cast(cp.school_year as int) as school_year, cp.class_period_name, cp.school_id,
    brule.tdoe_error_code as error_code,
    concat('Class Period ', cp.class_period_name, ' has more than one meeting time. Meeting Times: ', cast(cp.v_meeting_times as String)) as error,
    brule.tdoe_severity as severity
from stg_class_periods cp
join tooManyClassPeriods x
    on x.k_class_period = cp.k_class_period
join brule
    on cp.school_year between brule.error_school_year_start and brule.error_school_year_end
