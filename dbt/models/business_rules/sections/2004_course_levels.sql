{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 2004 %}

with stg_sections as (
    select * from {{ ref('stg_ef3__sections_orig') }}
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
    {{ error_code }} as error_code,
    concat('This Section can be designated with only one of the following submitted course levels: "Honors", "Statewide Dual Credit", "Local Dual Credit", "Dual Enrollment". Values received: ', 
        clc.courseLevels) as error,
    {{ error_severity_column(error_code, 's') }}
from stg_sections s
join courseLevelCounts clc
    on clc.k_course_section = s.k_course_section