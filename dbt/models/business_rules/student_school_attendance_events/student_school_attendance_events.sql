{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

select *
from {{ ref('3300_ssd_dates_within_enrollment') }}
union
select *
from {{ ref('3301_attendance_dates_within_enrollment') }}
union
select *
from {{ ref('3302_missing_ssd_coverage') }}
union
select *
from {{ ref('3303_bad_ssd_durations') }}