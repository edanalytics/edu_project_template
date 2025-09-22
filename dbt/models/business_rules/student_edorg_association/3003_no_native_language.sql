{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3003 %}

/* Students are required to have Native Language. */
with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
stg_student_edorgs as (
    select *
    from {{ ref('stg_ef3__student_education_organization_associations_orig') }} seoa
    where k_lea is not null
        and exists (
        select 1
        from brule
        where cast(seoa.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    )
)
select se.k_student, se.k_lea, se.k_school, se.school_year, se.ed_org_id, se.student_unique_id,
    s.state_student_id as legacy_state_student_id,
    brule.tdoe_error_code as error_code,
    concat('Native Language for Student ', 
        se.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
        'is required on District level Student/EdOrg Associations.') as error,
    brule.tdoe_severity as severity
from stg_student_edorgs se
join {{ ref('edu_edfi_source', 'stg_ef3__students') }} s
    on se.k_student = s.k_student
join brule
    on se.school_year between brule.error_school_year_start and brule.error_school_year_end
where 
    not exists (
            select 1
            from {{ ref('stg_ef3__stu_ed_org__languages') }} sl
            where sl.k_lea = se.k_lea
                and sl.k_student = se.k_student
                and sl.language_use in ('Native language', 'Home language', 'Dominant language')
        )