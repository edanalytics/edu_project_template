{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3007 %}

/* Student Characteristics cannot overlap. */
with stg_student_edorgs as (
    select *
    from {{ ref('stg_ef3__student_education_organization_associations_orig') }} seoa
    where k_lea is not null
        {{ school_year_exists(error_code, 'seoa') }}
),
characteristics_to_compare as (
    select *
    from (
        select se.k_student, se.k_lea, se.k_school, se.school_year, se.ed_org_id, se.student_unique_id,
            sc.student_characteristic, sc.begin_date, sc.end_date, 
            ifnull(sc.end_date, to_date('9999-12-31', 'yyyy-MM-dd')) as safe_end_date,
            count(*) over (partition by se.k_student, se.k_lea, sc.student_characteristic) as characteristic_count
        from stg_student_edorgs se
        join {{ ref('stg_ef3__stu_ed_org__characteristics') }} sc
            on sc.k_lea = se.k_lea
            and sc.k_student = se.k_student
            and (sc.end_date is null
                or sc.begin_date <= sc.end_date)
    ) x
    where x.characteristic_count > 1
)
select a.k_student, a.k_lea, a.k_school, a.school_year, a.ed_org_id, a.student_unique_id,
    {{ error_code }} as error_code,
    concat('Same Student Characteristics are not allowed to overlap. Values received: ',
        concat(a.student_characteristic, ' [', a.begin_date, ' - ', ifnull(a.end_date, 'null'), ']'),
        ', ',
        concat(b.student_characteristic, ' [', b.begin_date, ' - ', ifnull(b.end_date, 'null'), ']')
    ) as error,
    {{ error_severity_column(error_code, 'a') }}
from characteristics_to_compare a
join characteristics_to_compare b
    on b.k_lea = a.k_lea
    and b.k_student = a.k_student
    and b.student_characteristic = a.student_characteristic
    and b.begin_date > a.begin_date
    /* This looks for overlapping dates. */
    and (a.begin_date <= b.safe_end_date) and (a.safe_end_date >= b.begin_date)