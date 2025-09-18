{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 2010 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
stg_sections as (
    select * from {{ ref('stg_ef3__sections_orig') }} s
    where exists (
        select 1
        from brule
        where cast(s.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    )
)
/* Sections must have an educational_environment_type. */
select s.k_course_section, s.k_course_offering, s.k_school, s.k_location, s.k_school__location, 
    s.section_id, s.local_course_code, s.school_id, s.school_year, s.session_name,
    brule.tdoe_error_code as error_code,
    concat('Educational environment designation is blank for Section. ', s.local_course_code, ', ', s.section_id, ', ', session_name, '.') as error,
    brule.tdoe_severity as severity
from stg_sections s
join brule
    on s.school_year between brule.error_school_year_start and brule.error_school_year_end
where s.educational_environment_type is null