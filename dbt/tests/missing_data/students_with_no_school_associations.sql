/*
Find students who do not have a school association.
*/
{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
-- find students with school enrollments
select distinct stu.tenant_code, stu.api_year, stu.k_student
from {{ ref('stg_ef3__students') }} stu
left outer join {{ ref('stg_ef3__student_school_associations') }} ssa
  on stu.k_student = ssa.k_student
where ssa.k_student is null