{{
  config(
    materialized="table",
    schema="stg_adm_gaps"
  )
}}

with unioned (
    select * from {{ ref('wrk_adm_gaps_calendar_issues') }}
    union all
    select * from {{ ref('wrk_adm_gaps_class_period_issues') }}
    union all
    select * from {{ ref('wrk_adm_gaps_edorg_assoc_issues') }}
    union all
    select * from {{ ref('wrk_adm_gaps_enrollment_issues') }}
    union all
    select * from {{ ref('wrk_adm_gaps_expulsions') }}
    union all
    select * from {{ ref('wrk_adm_gaps_funding_ineligible') }}
    union all
    select * from {{ ref('wrk_adm_gaps_sections_issues') }}
    union all
    select * from {{ ref('wrk_adm_gaps_sessions_issues') }}
    union all
    select * from {{ ref('wrk_adm_gaps_ssd_issues') }}
    union all
    select * from {{ ref('wrk_adm_gaps_student_schedule_issues') }}
)/*,
grouped as (
    select school_year, k_student, k_school, is_primary_school,
        concat_ws('\n', collect_list(concat(possible_reason))) as possible_reasons
    from unioned
    group by school_year, k_student, k_school, is_primary_school
)*/
select g.school_year, stu.student_unique_id, stu.state_student_id,
    sch.school_name, sch.school_id, lea.lea_name as district_name, lea.lea_id as district_number,
    g.is_primary_school, g.reason_type, g.reason_count, g.possible_reason,
    g.k_student, g.k_school
from unioned g
left outer join {{ ref('stg_ef3__students') }} stu
    on stu.k_student = g.k_student
join {{ ref('stg_ef3__schools') }} sch
    on sch.k_school = g.k_school
join {{ ref('stg_ef3__local_education_agencies') }} lea
    on lea.k_lea = sch.k_lea