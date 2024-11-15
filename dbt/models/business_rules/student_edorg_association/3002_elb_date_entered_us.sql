{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3002 %}

/* Students with below characteristics must have Date Entered US populated. */
with stg_student_edorgs as (
    select *
    from {{ ref('stg_ef3__student_education_organization_associations_orig') }} seoa
    where k_lea is not null
        {{ school_year_exists(error_code, 'seoa') }}
)
select se.k_student, se.k_lea, se.k_school, se.school_year, se.ed_org_id, se.student_unique_id,
    {{ error_code }} as error_code,
    concat('ELB Students with codes [L, W, 1, 2, 3, 4, F, N] require Date Entered US on District level Student/EdOrg Associations.') as error,
    {{ error_severity_column(error_code, 'se') }}
from stg_student_edorgs se
join {{ ref('edu_edfi_source', 'stg_ef3__students') }} s
    on se.k_student = s.k_student
where s.date_entered_us is null
    and exists (
            select 1
            from {{ ref('stg_ef3__stu_ed_org__characteristics') }} sc
            where sc.k_lea = se.k_lea
                and sc.k_student = se.k_student
                and sc.student_characteristic in ('L','W','1','2','3','4','F','N')
        )