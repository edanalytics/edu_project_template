{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

select *
from {{ ref('2007_meeting_time_count') }}
union
select *
from {{ ref('2009_meeting_time_duration') }}
union
select *
from {{ ref('2011_no_bell_schedule_assoc') }}