{{
  config(
    materialized="table",
    schema="stg_adm_gaps"
  )
}}

with bad_sessions as (
    select sess.school_year, sch.k_school,
        count(*) as reason_count,
        concat_ws('\n', collect_list(concat('\t', error))) as errors
    from {{ ref('sessions') }} sess
    join {{ ref('stg_ef3__schools') }} sch
        on sch.school_id = sess.school_id
    where severity = 'critical'
    group by sess.school_year, sch.k_school
)
select school_year, null as k_student, k_school, null as is_primary_school,
    'session' as reason_type,
    reason_count,
    concat('School has the following Session errors:\n', errors) as possible_reason
from bad_sessions
union
select e.school_year, e.k_student, e.k_school, e.is_primary_school,
    'session' as reason_type,
    reason_count,
    concat('Student is tied to Sessions with the following errors:\n', errors) as possible_reason
from {{ ref('adm_gaps_enrollments') }} e
join bad_sessions s
    on s.school_year = e.school_year
    and s.k_school = e.k_school