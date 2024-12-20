{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 2002 %}

with stg_sessions as (
    select * from {{ ref('stg_ef3__sessions_orig') }} s
    where 1=1
        {{ school_year_exists(error_code, 's') }}
)
/* Session End Date must be within the school year begin and end date. */
select s.k_session, s.session_name, s.school_id, s.school_year, s.session_begin_date as begin_date, s.session_end_date as end_date,
    s.academic_term, s.total_instructional_days,
    {{ error_code }} as error_code,
    concat('Session End Date does not fall within the school year. Value Received: ', s.session_end_date, '. The state school year starts ',
      concat((s.school_year-1), '-07-01'), ' and ends ', concat(s.school_year, '-06-30'), '.') as error,
    {{ error_severity_column(error_code, 's') }}
from stg_sessions s
where not(s.session_end_date between to_date(concat((s.school_year-1), '-07-01'), 'yyyy-MM-dd') 
    and to_date(concat(s.school_year, '-06-30'), 'yyyy-MM-dd'))