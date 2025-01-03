{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

select *
from {{ ref('1000_not_enough_instructional_days') }}
union
select *
from {{ ref('1001_events_requiring_an_ID_event') }}
union
select *
from {{ ref('1002_events_that_cannot_have_ID_event') }}
union
select *
from {{ ref('1003_calendar_date_not_within_schoolyear') }}
union
select *
from {{ ref('1004_required_calendar_events') }}
union
select *
from {{ ref('1005_inservice_day_count') }}
union
select *
from {{ ref('1006_abbreviated_day_count') }}
union
select *
from {{ ref('1007_parent_teacher_day_count') }}
union
select *
from {{ ref('1008_discrectionary_day_count') }}