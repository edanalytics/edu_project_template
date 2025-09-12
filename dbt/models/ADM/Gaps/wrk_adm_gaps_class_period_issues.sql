{{
  config(
    materialized="table",
    schema="stg_adm_gaps"
  )
}}

with q as (
    select sess.school_year, sch.k_school, 
        count(*) as reason_count,
        concat_ws('\n', collect_list(concat('\t', error))) as errors
    from {{ ref('class_periods') }} sess
    join {{ ref('stg_ef3__schools') }} sch
        on sch.school_id = sess.school_id
    where severity = 'critical'
    group by sess.school_year, sch.k_school
)
select school_year, null as k_student, k_school, null as is_primary_school,
    'class period' as reason_type,
    reason_count,
    concat('School has the following Class Period errors:\n', errors) as possible_reason
from q
union
select e.school_year, e.k_student, e.k_school, e.is_primary_school,
    'class period' as reason_type,
    count(*) as reason_count,
    concat('Student is tied to Class Periods with the following errors:\n', 
        concat_ws('\n', collect_list(concat('\t', x.error)))) as possible_reason
from {{ ref('adm_gaps_enrollments') }} e
join {{ ref('stg_ef3__student_section_associations_orig')}} sections
    on sections.school_year = e.school_year
    and sections.k_student = e.k_student
    and sections.school_id = e.school_id
join {{ ref('stg_ef3__sections__class_periods') }} cps
    on cps.k_course_section = sections.k_course_section
join {{ ref('class_periods') }} x
    on x.k_class_period = cps.k_class_period
group by e.school_year, e.k_student, e.k_school, e.is_primary_school