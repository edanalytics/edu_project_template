{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3003 %}

/* Students are required to have Native Language. */
with stg_student_edorgs as (
    select *
    from {{ ref('stg_ef3__student_education_organization_associations_orig') }} seoa
    where k_lea is not null
        {{ school_year_exists(error_code, 'seoa') }}
)
select se.k_student, se.k_lea, se.k_school, se.school_year, se.ed_org_id, se.student_unique_id,
    {{ error_code }} as error_code,
    concat('Native Language is required on District level Student/EdOrg Associations.') as error,
    {{ error_severity_column(error_code, 'se') }}
from stg_student_edorgs se
where 
    not exists (
            select 1
            from {{ ref('stg_ef3__stu_ed_org__languages') }} sl
            where sl.k_lea = se.k_lea
                and sl.k_student = se.k_student
                and sl.language_use = 'Native language'
        )