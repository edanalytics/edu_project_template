{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3601 %}

with stg_course_transcripts as (
    select * from {{ ref('stg_ef3__course_transcripts_orig') }} ct
    where 1=1
        {{ school_year_exists(error_code, 'ct') }}
)
/* Final Numeric Grades must be from a set of accepted values. */
select ct.k_course, ct.k_student_academic_record, ct.school_year, ct.course_attempt_result, ct.course_code, 
    ct.course_ed_org_id, ct.student_academic_record_ed_org_id, ct.student_unique_id, ct.academic_term,
    ct.alternative_course_code,
    {{ error_code }} as error_code,
    concat('Final Numeric Grade must be between 0 and 105 (inclusive) or [null]. Final Numeric Grade Received: ',
        ifnull(ct.final_numeric_grade_earned, '[null]'), '.') as error,
    {{ error_severity_column(error_code, 'ct') }}
from stg_course_transcripts ct
where ct.final_numeric_grade_earned is not null
    and (ct.final_numeric_grade_earned < 0
        or ct.final_numeric_grade_earned > 105)