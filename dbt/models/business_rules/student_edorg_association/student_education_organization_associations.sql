{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

select *
from {{ ref('3001_immigrant_date_entered_us') }}
union
select *
from {{ ref('3002_elb_date_entered_us') }}
union
select *
from {{ ref('3003_no_native_language') }}
union
select *
from {{ ref('3004_no_race') }}
union
select *
from {{ ref('3005_cohort_year') }}
union
select *
from {{ ref('3006_student_characteristics_end_date') }}
union
select *
from {{ ref('3007_student_characteristics_overlaps') }}