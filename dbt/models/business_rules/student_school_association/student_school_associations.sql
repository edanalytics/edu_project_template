{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

select *
from {{ ref('3101_entry_date') }}
union
select *
from {{ ref('3102_exit_withdrawal_date') }}
union
select *
from {{ ref('3103_entry_exit_withdrawal_date') }}
union
select *
from {{ ref('3104_S_enrollment_w_no_P_enrollment') }}
union
select *
from {{ ref('3105_overlapping_P_enrollments') }}