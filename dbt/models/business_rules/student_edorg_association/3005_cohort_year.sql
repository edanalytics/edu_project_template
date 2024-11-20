{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3005 %}

/* 9th grade Students and above are required to have one value for Cohort Year. */
with stg_student_edorgs as (
    select *
    from {{ ref('stg_ef3__student_education_organization_associations_orig') }} seoa
    where k_lea is not null
        {{ school_year_exists(error_code, 'seoa') }}
)
select se.k_student, se.k_lea, se.k_school, se.school_year, se.ed_org_id, se.student_unique_id,
    {{ error_code }} as error_code,
    concat('A single value of Cohort Year is required on District level Student/EdOrg Associations for Students with instructional grade greater than 8th. Values Received: ', 
        cast(se.v_cohort_years as String), ', Student Grade: ', ssa.entry_grade_level) as error,
    {{ error_severity_column(error_code, 'se') }}
from stg_student_edorgs se
join {{ ref('edu_edfi_source', 'stg_ef3__schools') }} s
    on s.k_lea = se.k_lea
join {{ ref('stg_ef3__student_school_associations_orig') }} ssa
    on ssa.k_student = se.k_student
    and ssa.k_school = s.k_school
join {{ ref('xwalk_grade_levels') }} gl
    on gl.grade_level = ssa.entry_grade_level
    and gl.grade_level_integer between 9 and 12
where size(cast(se.v_cohort_years as array<string>)) != 1