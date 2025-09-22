{{
  config(
    materialized="table",
    schema="stg_adm_gaps"
  )
}}

select e.school_year, e.k_student, e.k_school, e.is_primary_school,
    'edorg assoc' as reason_type,
    reason_count,
    concat('Student has errors with their School Associations:\n', x.errors) as possible_reason
from {{ ref('adm_gaps_enrollments') }} e
join (
        select school_year, k_student, k_lea,
            count(*) as reason_count,
            concat_ws('\n', collect_list(concat('\t', error))) as errors
        from {{ ref('student_education_organization_associations')}}
        where severity = 'critical'
        group by school_year, k_student, k_lea
    ) x
    on x.school_year = e.school_year
    and x.k_student = e.k_student
    and x.k_lea = e.k_lea