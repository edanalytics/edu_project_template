{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

with stg_sessions as (
    select * from {{ ref('stg_ef3__sessions') }}
)
/* Session Begin Date must be within the school year begin and end date. */
select k_session, session_name, school_id, school_year, session_begin_date as begin_date, session_end_date as end_date,
    academic_term, total_instructional_days,
    2001 as error_code,
    concat('Session Begin Date does not fall within the school year. Value Received: ', session_begin_date, '. The state school year starts ',
      concat((school_year-1), '-07-01'), ' and ends ', concat(school_year, '-06-30'), '.') as error
from stg_sessions
where not(session_begin_date between to_date(concat((school_year-1), '-07-01'), 'yyyy-MM-dd') 
    and to_date(concat(school_year, '-06-30'), 'yyyy-MM-dd'))
union
/* Session End Date must be within the school year begin and end date. */
select k_session, session_name, school_id, school_year, session_begin_date as begin_date, session_end_date as end_date,
    academic_term, total_instructional_days,
    2002 as error_code,
    concat('Session End Date does not fall within the school year. Value Received: ', session_end_date, '. The state school year starts ',
      concat((school_year-1), '-07-01'), ' and ends ', concat(school_year, '-06-30'), '.') as error
from stg_sessions
where not(session_end_date between to_date(concat((school_year-1), '-07-01'), 'yyyy-MM-dd') 
    and to_date(concat(school_year, '-06-30'), 'yyyy-MM-dd'))
union
/* Session End Date must be >= Session Begin Date. */
select k_session, session_name, school_id, school_year, session_begin_date as begin_date, session_end_date as end_date,
    academic_term, total_instructional_days,
    2003 as error_code,
    concat('Session End Date must be greater than or equal to Session Begin Date. Value received: : ', 
      session_end_date, '. Session Begin Date ', session_begin_date, '.') as error
from stg_sessions
where session_end_date < session_begin_date