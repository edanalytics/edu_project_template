{{
  config(
    materialized="table",
    schema="cds"
  )
}}

select s.k_student, 
    s.state_student_id as stateStudentId
from {{ ref('stg_ef3__students') }} s
where s.state_student_id is not null