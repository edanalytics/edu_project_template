{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3006 %}

/* Student Characteristic dates must make sense. */
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
    concat('Student Characteristics End Date for Student ', 
        se.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
        ' must be greater than or equal to Begin Date. Value received: ',
        concat(sc.student_characteristic, ' [', sc.begin_date, ' - ', sc.end_date, ']')) as error,
    brule.tdoe_severity as severity
from stg_student_edorgs se
join {{ ref('edu_edfi_source', 'stg_ef3__students') }} s
    on se.k_student = s.k_student
join {{ ref('stg_ef3__stu_ed_org__characteristics') }} sc
    on sc.k_lea = se.k_lea
    and sc.k_student = se.k_student
    and sc.end_date is not null
    and sc.end_date < begin_date
join brule
    on se.school_year between brule.error_school_year_start and brule.error_school_year_end