{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 2005 %}

with stg_sections as (
    select * from {{ ref('stg_ef3__sections_orig') }}
),
sectionsCharacteristicsExploded as (
    select distinct k_course_section, 
        {{ edu_edfi_source.extract_descriptor('value:sectionCharacteristicDescriptor::string') }} as sectionCharacteristic
    from stg_sections, 
        lateral variant_explode(v_section_characteristics)
),
sectionScheduleTypes as (
    select *
    from (
        select k_course_section, 
            count(*) as sectionScheduleTypeCount,
            cast(array_agg(sectionCharacteristic) as String) as sectionSchedules
        from sectionsCharacteristicsExploded
        where sectionCharacteristic in ('F', 'S', 'T')
        group by k_course_section
    ) x
    where sectionScheduleTypeCount > 1
)
/* Sections only get one of F or S or T. */
select s.k_course_section, s.k_course_offering, s.k_school, s.k_location, s.k_school__location, 
    s.section_id, s.local_course_code, s.school_id, s.school_year, s.session_name,
    {{ error_code }} as error_code,
    concat('Section can only have one of the following value for Test Admin Window: F or S or T. Values received: ', 
        sst.sectionSchedules) as error,
    {{ error_severity_column(error_code, 's') }}
from stg_sections s
join sectionScheduleTypes sst
    on sst.k_course_section = s.k_course_section