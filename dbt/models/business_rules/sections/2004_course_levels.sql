{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 2004 %}

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
),
courseLevelsExploded as (
    select distinct k_course_section,
        {{ edu_edfi_source.extract_descriptor('value:courseLevelCharacteristicDescriptor::string') }} as courseLevelCharacteristic
    from stg_sections, 
        lateral variant_explode(v_course_level_characteristics)
    where size(cast(v_course_level_characteristics as array<string>)) > 1
), 
courseLevelCounts as (
    select k_course_section, cast(courseLevels as String) as courseLevels
    from (
        select k_course_section, 
            count(*) as courseLevelCount,
            array_agg(courseLevelCharacteristic) as courseLevels
        from courseLevelsExploded
        where courseLevelCharacteristic in ('Honors', 'Statewide Dual Credit', 'Local Dual Credit', 'Dual Enrollment')
        group by k_course_section
    ) x
    where courseLevelCount > 1
)
/* Sections only get one of "Honors", "Statewide Dual Credit", "Local Dual Credit", "Dual Enrollment". */
select s.k_course_section, s.k_course_offering, s.k_school, s.k_location, s.k_school__location, 
    s.section_id, s.local_course_code, s.school_id, s.school_year, s.session_name,
    brule.tdoe_error_code as error_code,
    concat('Section ', s.section_id, ' can be designated with only one of the following submitted course levels: "Honors", "Statewide Dual Credit", "Local Dual Credit", "Dual Enrollment". Values received: ', 
        clc.courseLevels) as error,
    brule.tdoe_severity as severity
from stg_sections s
join courseLevelCounts clc
    on clc.k_course_section = s.k_course_section
join brule
    on s.school_year between brule.error_school_year_start and brule.error_school_year_end