{{
  config(
    materialized="table",
    schema="stg_adm_gaps"
  )
}}

with bad as (
    select school_year, k_school,
        count(*) as reason_count, 
        concat_ws('\n', collect_list(concat('\t', error))) as errors
    from {{ ref('sections') }} x
    where severity = 'critical'
    group by school_year, k_school
)
select school_year, null as k_student, k_school, null as is_primary_school,
    'section' as reason_type,
    reason_count,
    concat('School has the following Sections errors:\n', errors) as possible_reason
from bad
union all
select e.school_year, e.k_student, e.k_school, e.is_primary_school,
    'section' as reason_type,
    count(*) as reason_count,
    concat('Student tied to Sections with the following errors:\n', 
        concat_ws('\n', collect_list(concat('\t', x.error)))) as possible_reason
from {{ ref('adm_gaps_enrollments') }} e
join {{ ref('stg_ef3__student_section_associations_orig')}} sections
    on sections.school_year = e.school_year
    and sections.k_student = e.k_student
join {{ ref('sections') }} x
    on x.k_course_section = sections.k_course_section
    and x.severity = 'critical'
group by e.school_year, e.k_student, e.k_school, e.is_primary_school
union all
select e.school_year, e.k_student, e.k_school, e.is_primary_school,
    'section' as reason_type,
    reason_count,
    concat('Student has errors with their Section Associations:\n', sections.errors) as possible_reason
from {{ ref('adm_gaps_enrollments') }} e
join (
        select school_year, k_student, school_id,
            count(*) as reason_count,
            concat_ws('\n', collect_list(concat('\t', error))) as errors
        from {{ ref('student_section_associations')}}
        where severity = 'critical'
        group by school_year, k_student, school_id
    ) sections
    on sections.school_year = e.school_year
    and sections.k_student = e.k_student
    and sections.school_id = e.school_id
union all
select e.school_year, e.k_student, e.k_school, e.is_primary_school,
    'section' as reason_type,
    1 as reason_count,
    concat('Student has no Section Associations.') as possible_reason
from {{ ref('adm_gaps_enrollments') }} e
where not exists (
    select 1
    from {{ ref('stg_ef3__student_section_associations_orig')}} sections
    where sections.school_year = e.school_year
        and sections.k_student = e.k_student
)