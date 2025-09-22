{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 2008 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
stg_bell_schedules as (
    select * from {{ ref('stg_ef3__bell_schedules_orig') }} bs
    where exists (
        select 1
        from brule
        where cast(bs.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    )
),
missingDates as (
    select k_bell_schedule,
        size(cast(v_dates as array<string>)) dateCount
    from stg_bell_schedules
    where size(cast(v_dates as array<string>)) = 0
)
/* Bell Schedules must have dates. */
select bs.k_bell_schedule, cast(bs.school_year as int) as school_year, bs.bell_schedule_name, bs.school_id,
    brule.tdoe_error_code as error_code,
    concat('Missing Bell Schedule DATES for Bell Schedule ', bs.bell_schedule_name, '.') as error,
    brule.tdoe_severity as severity
from stg_bell_schedules bs
join missingDates md
    on md.k_bell_schedule = bs.k_bell_schedule
join brule
    on bs.school_year between brule.error_school_year_start and brule.error_school_year_end