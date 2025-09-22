{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 2005 %}

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
        where upper(sectionCharacteristic) in ('FALL BLOCK', 'SPRING BLOCK', 'YEAR-LONG')
        group by k_course_section
    ) x
    where sectionScheduleTypeCount > 1
)
/* Sections only get one of 'FALL BLOCK', 'SPRING BLOCK', 'YEAR-LONG'. */
select s.k_course_section, s.k_course_offering, s.k_school, s.k_location, s.k_school__location, 
    s.section_id, s.local_course_code, s.school_id, s.school_year, s.session_name,
    brule.tdoe_error_code as error_code,
    concat('Section ', s.section_id, ' can only have one of the following value for Test Admin Window: Fall Block or Spring Block or Year-long. Values received: ', 
        sst.sectionSchedules) as error,
    brule.tdoe_severity as severity
from stg_sections s
join sectionScheduleTypes sst
    on sst.k_course_section = s.k_course_section
join brule
    on s.school_year between brule.error_school_year_start and brule.error_school_year_end