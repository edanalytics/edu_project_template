{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3600 %}

with stg_course_transcripts as (
    select * from {{ ref('stg_ef3__course_transcripts_orig') }} ct
    where 1=1
        {{ school_year_exists(error_code, 'ct') }}
)
/* Final Letter Grades must be from a set of accepted values. */
select ct.k_course, ct.k_student_academic_record, ct.school_year, ct.course_attempt_result, ct.course_code, 
    ct.course_ed_org_id, ct.student_academic_record_ed_org_id, ct.student_unique_id, ct.academic_term,
    ct.alternative_course_code,
    {{ error_code }} as error_code,
    concat('Final Letter Grade must be one of A-, A, A+, B-, B, B+, C-, C, C+, D-, D, D+, F, I, P, [null]. Final Letter Grade Received: ',
        ifnull(ct.final_letter_grade_earned, '[null]'), '.') as error,
    {{ error_severity_column(error_code, 'ct') }}
from stg_course_transcripts ct
where ct.final_letter_grade_earned is not null
    and ct.final_letter_grade_earned not in ('A-', 'A', 'A+', 'B-', 'B', 'B+', 'C-', 'C', 'C+', 'D-', 'D', 'D+', 'F', 'I', 'P')