{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 2003 %}

with stg_sessions as (
    select * from {{ ref('stg_ef3__sessions_orig') }}
)
/* Session End Date must be >= Session Begin Date. */
select s.k_session, s.session_name, s.school_id, s.school_year, s.session_begin_date as begin_date, s.session_end_date as end_date,
    s.academic_term, s.total_instructional_days,
    {{ error_code }} as error_code,
    concat('Session End Date must be greater than or equal to Session Begin Date. Value received: : ', 
      s.session_end_date, '. Session Begin Date ', s.session_begin_date, '.') as error,
    {{ error_severity_column(error_code, 's') }}
from stg_sessions s
where s.session_end_date < s.session_begin_date