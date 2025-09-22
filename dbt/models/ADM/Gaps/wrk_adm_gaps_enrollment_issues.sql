{{
  config(
    materialized="table",
    schema="stg_adm_gaps"
  )
}}

select school_year, k_student, k_school, is_primary_school,
    'enrollment' as reason_type,
    1 as reason_count,
    concat('Student only enrolled for ', enrolled_days, ' days (', entry_date, ' - ', coalesce(exit_withdraw_date, '[no end date]'), ').') as possible_reason
from {{ ref('adm_gaps_enrollments') }}
where enrolled_days < 180
union all
select e.school_year, e.k_student, e.k_school, e.is_primary_school,
    'enrollment' as reason_type,
    reason_count,
    concat('Student has errors with their School Associations:\n', x.errors) as possible_reason
from {{ ref('adm_gaps_enrollments') }} e
join (
        select school_year, k_student, school_id,
            count(*) as reason_count,
            concat_ws('\n', collect_list(concat('\t', error))) as errors
        from {{ ref('student_school_associations')}}
        where severity = 'critical'
        group by school_year, k_student, school_id
    ) x
    on x.school_year = e.school_year
    and x.k_student = e.k_student
    and x.school_id = e.school_id