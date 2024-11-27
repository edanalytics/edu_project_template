{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

select *
from {{ ref('2004_course_levels') }}
union
select *
from {{ ref('2005_test_admin_window') }}
union
select *
from {{ ref('2006_non_pullout_need_duration') }}
union
select *
from {{ ref('2010_educational_environment_type') }}