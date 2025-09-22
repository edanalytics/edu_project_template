{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 2003 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
stg_sessions as (
    select * from {{ ref('stg_ef3__sessions_orig') }} s
    where exists (
        select 1
        from brule
        where cast(s.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    )
)
/* Session End Date must be >= Session Begin Date. */
select s.k_session, s.session_name, s.school_id, s.school_year, s.session_begin_date as begin_date, s.session_end_date as end_date,
    s.academic_term, s.total_instructional_days,
    brule.tdoe_error_code as error_code,
    concat('Session End Date must be greater than or equal to Session Begin Date. Value received: : ', 
      s.session_end_date, '. Session Begin Date ', s.session_begin_date, '.') as error,
    brule.tdoe_severity as severity
from stg_sessions s
join brule
    on s.school_year between brule.error_school_year_start and brule.error_school_year_end
where s.session_end_date < s.session_begin_date