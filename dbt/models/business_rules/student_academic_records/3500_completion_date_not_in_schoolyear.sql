{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3500 %}

with stg_student_academic_records as (
    select * from {{ ref('stg_ef3__student_academic_records_orig') }} sar
    where 1=1
        {{ school_year_exists(error_code, 'sar') }}
),
stg_student_academic_records__diplomas as (
    select * from {{ ref('stg_ef3__student_academic_records__diplomas') }} sard
)
/* Diploma Award Date must be within the school year begin and end date. */
select sar.k_student_academic_record, sar.k_student, sar.k_lea, sar.k_school, sar.ed_org_id,
    sar.school_year, sar.student_unique_id, sar.academic_term,
    sard.diploma_type, sard.diploma_description,
    s.state_student_id as legacy_state_student_id,
    {{ error_code }} as error_code,
    concat('Diploma Award Date does not fall within the school year for Student ', 
        sar.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
        'Value Received: ', 
        sard.diploma_award_date, '. The state school year starts ',
        concat((cast(sar.school_year as int)-1), '-07-01'), ' and ends ', concat(cast(sar.school_year as int), '-06-30'), '.') as error,
    {{ error_severity_column(error_code, 'sar') }}
from stg_student_academic_records sar
join stg_student_academic_records__diplomas sard
    on sard.k_student_academic_record = sar.k_student_academic_record
join {{ ref('stg_ef3__students') }} s
    on s.k_student = sar.k_student
where not(sard.diploma_award_date between to_date(concat(cast(sar.school_year as int)-1, '-07-01'), 'yyyy-MM-dd') 
        and to_date(concat(cast(sar.school_year as int), '-06-30'), 'yyyy-MM-dd'))