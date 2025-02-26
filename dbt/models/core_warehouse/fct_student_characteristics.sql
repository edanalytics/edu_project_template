{{
  config(
    materialized="table",
    schema="wh",
    post_hook=[
        "alter table {{ this }} alter column k_student set not null",
        "alter table {{ this }} alter column k_lea set not null",
        "alter table {{ this }} alter column student_characteristic set not null",
        "alter table {{ this }} alter column begin_date set not null",
        "alter table {{ this }} add primary key (k_student, k_lea, student_characteristic, begin_date)"
    ]
  )
}}

select c.tenant_code, c.k_student, c.k_student_xyear, c.ed_org_id, c.k_lea,
    c.student_characteristic, c.begin_date, c.end_date
from {{ ref('stg_ef3__stu_ed_org__characteristics') }} c
where c.k_lea is not null
    and c.student_characteristic is not null
    and c.begin_date is not null
    and not exists (
        select 1
        from {{ ref('xwalk_student_characteristics') }} x
        where upper(x.characteristic_descriptor) = upper(c.student_characteristic)
    )