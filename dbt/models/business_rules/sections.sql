{{
  config(
    materialized="table",
    schema="data_errors"
  )
}}

with stg_sections as (
    select * from {{ ref('stg_ef3__sections') }}
),
courseLevelsExploded as (
    select distinct k_course_section,
        {{ edu_edfi_source.extract_descriptor('value:sectionCharacteristicDescriptor::string') }} as courseLevelCharacteristic
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
), 
sectionsExploded as (
    select distinct k_course_section, 
        {{ edu_edfi_source.extract_descriptor('value:sectionCharacteristicDescriptor::string') }} as sectionCharacteristic
    from stg_sections, 
        lateral variant_explode(v_section_characteristics)
),
sectionScheduleTypes as (
    select k_course_section, 
        count(*) as sectionScheduleTypeCount,
        cast(array_agg(sectionCharacteristic) as String) as sectionSchedules
    from sectionsExploded
    where sectionCharacteristic in ('F', 'S', 'T')
    group by k_course_section
),
stg_sections_class_periods as (
    select s.k_course_section, s.educational_environment_type, cp.*
    from stg_sections s
    join {{ ref('stg_ef3__sections__class_periods') }} scp
        on scp.k_course_section = s.k_course_section
    join {{ ref('stg_ef3__class_periods') }} cp
        on cp.k_class_period = scp.k_class_period
),
nonPullOutsMissingClassPeriodDurations as (
    select k_course_section,
        cast(array_agg(class_period_name) as String) as class_periods
    from (
        select cp.k_course_section, 
            cp.class_period_name,
            v_meeting_times:[0].startTime::timestamp as start_time,
            v_meeting_times:[0].endTime::timestamp as end_time,
            timediff(MINUTE, start_time, end_time) as period_duration
        from stg_sections_class_periods cp
        where ifnull(cp.educational_environment_type,'X') != 'P'
    ) x
    where ifnull(period_duration, 0) <= 0
    group by k_course_section
)
/* Sections only get one of "Honors", "Statewide Dual Credit", "Local Dual Credit", "Dual Enrollment". */
select s.k_course_section, s.k_course_offering, s.k_school, s.k_location, s.k_school__location, 
    s.section_id, s.local_course_code, s.school_id, s.school_year, s.session_name,
    2004 as error_code,
    concat('This Section can be designated with only one of the following submitted course levels: "Honors", "Statewide Dual Credit", "Local Dual Credit", "Dual Enrollment". Values received: ', 
        clc.courseLevels) as error
from stg_sections s
join courseLevelCounts clc
    on clc.k_course_section = s.k_course_section
union
/* Sections only get one of F or S or T. */
select s.k_course_section, s.k_course_offering, s.k_school, s.k_location, s.k_school__location, 
    s.section_id, s.local_course_code, s.school_id, s.school_year, s.session_name,
    2005 as error_code,
    concat('WARNING:- Section can only have one of the following value for Test Admin Window: F or S or T. Values Recieved: ', 
        ifnull(sst.sectionSchedules, '[]')) as error
from stg_sections s
left outer join sectionScheduleTypes sst
    on sst.k_course_section = s.k_course_section
where sst.sectionScheduleTypeCount is null 
    or sst.sectionScheduleTypeCount != 1
union
/* Sections that are not Pull Out must have a meeting time duration. */
select s.k_course_section, s.k_course_offering, s.k_school, s.k_location, s.k_school__location, 
    s.section_id, s.local_course_code, s.school_id, s.school_year, s.session_name,
    2006 as error_code,
    concat('Sections with an Educational Environment Descriptor of "', ifnull(s.educational_environment_type, 'null'), 
        '" must submit Class Periods with valid durations. Class Periods with invalid durations: ', x.class_periods) as error
from stg_sections s
join nonPullOutsMissingClassPeriodDurations x
    on x.k_course_section = s.k_course_section
union
/* Sections must have an educational_environment_type. */
select s.k_course_section, s.k_course_offering, s.k_school, s.k_location, s.k_school__location, 
    s.section_id, s.local_course_code, s.school_id, s.school_year, s.session_name,
    2010 as error_code,
    concat('Educational environment designation is blank for section. ', s.local_course_code, ', ', s.section_id, ', ', session_name, '.') as error
from stg_sections s
where s.educational_environment_type is not null