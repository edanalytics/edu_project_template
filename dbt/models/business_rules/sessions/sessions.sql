{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

select *
from {{ ref('2001_begin_date') }}
union
select *
from {{ ref('2002_end_date') }}
union
select *
from {{ ref('2003_begin_end_date_valid') }}