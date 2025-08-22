{{
  config(
    materialized="table",
    schema="stg_adm"
  )
}}

/* 
The purpose of this model is to get all the Course Sections and Days that meet that contribute to ADM, 
along with the duration of the Section (which can be split across multiple class periods).
*/

select dcs.school_year, dcs.k_school, dcs.k_course_section, dcs.course_code, dcs.is_cte,
    scpd.calendar_date, sum(scpd.period_duration) as period_duration
from {{ ref('dim_course_section') }} dcs
join {{ ref('fct_section_class_period_dates') }} scpd
    on scpd.school_year = dcs.school_year
    and scpd.k_school = dcs.k_school
    and scpd.k_course_section = dcs.k_course_section
where dcs.course_code not in ('G25H09','G25X23') /* Remove cafeteria courses */
    and ifnull(dcs.educational_environment_type,'X') != 'P' /* Remove pull out classes. */
group by dcs.school_year, dcs.k_school, dcs.k_course_section, dcs.course_code, dcs.is_cte,
    scpd.calendar_date