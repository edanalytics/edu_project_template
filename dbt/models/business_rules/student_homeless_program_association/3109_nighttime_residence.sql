{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3109 %}

with stg_homeless as (
    select * 
    from {{ ref('stg_ef3__student_homeless_program_associations_orig') }} sh
    where sh.k_lea is not null
        {{ school_year_exists(error_code, 'sh') }} 
)
/* Homeless Students must have a Primary Nighttime Residence. */
select h.k_student, h.k_program, h.k_lea, h.school_year, h.student_unique_id, h.ed_org_id, h.program_enroll_begin_date,
    s.state_student_id as legacy_state_student_id,
    {{ error_code }} as error_code,
    concat('Homeless Primary Nighttime Residence for Student ', 
        h.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
        'cannot be blank.') as error,
    {{ error_severity_column(error_code, 'h') }}
from stg_homeless h
join {{ ref('stg_ef3__students') }} s
    on s.k_student = h.k_student
where h.homeless_primary_nighttime_residence is null