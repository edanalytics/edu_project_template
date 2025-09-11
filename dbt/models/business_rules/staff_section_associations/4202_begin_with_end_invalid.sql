{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 4202 %}

with stg_staff_section_associations as (
    select * from {{ ref('stg_ef3__staff_section_associations_orig') }} ssa
    where ssa.end_date is not null
        {{ school_year_exists(error_code, 'ssa') }}
)
/* End Date must be greater than Begin Date. */
select ssa.k_staff, ssa.k_course_section, ssa.local_course_code, ssa.school_year, ssa.school_id, 
    ssa.section_id, ssa.session_name, ssa.staff_unique_id, ssa.begin_date,
    {{ error_code }} as error_code,
    concat('Staff Section Assocition End Date must be greater than the Begin Date. End Date received: ',
        ssa.end_date, ', Begin Date: ', ssa.end_date, '.') as error,
    {{ error_severity_column(error_code, 'ssa') }}
from stg_staff_section_associations ssa
where ssa.end_date < ssa.begin_date