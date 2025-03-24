{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 2010 %}

with stg_sections as (
    select * from {{ ref('stg_ef3__sections_orig') }} s
    where 1=1
        {{ school_year_exists(error_code, 's') }}
)
/* Sections must have an educational_environment_type. */
select s.k_course_section, s.k_course_offering, s.k_school, s.k_location, s.k_school__location, 
    s.section_id, s.local_course_code, s.school_id, s.school_year, s.session_name,
    {{ error_code }} as error_code,
    concat('Educational environment designation is blank for section. ', s.local_course_code, ', ', s.section_id, ', ', session_name, '.') as error,
    {{ error_severity_column(error_code, 's') }}
from stg_sections s
where s.educational_environment_type is null