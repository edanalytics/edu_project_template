{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 4100 %}

with stg_staff_edorg_assignment_assoc as (
    select * from {{ ref('stg_ef3__staff_education_organization_assignment_associations_orig') }} seoas
    where 1=1
        {{ school_year_exists(error_code, 'seoas') }}
)
/* Full Time Equivalency must be null, 1, 2, or 3. */
select seaa.k_staff, seaa.k_lea, seaa.k_school, seaa.school_year, seaa.ed_org_id, seaa.staff_unique_id,
    seaa.begin_date, seaa.staff_classification,
    {{ error_code }} as error_code,
    concat('Full Time Equivalency can be [null] or between 0 and 1. Value Received: ', 
        ifnull(seaa.full_time_equivalency, '[null]'), '.') as error,
    {{ error_severity_column(error_code, 'seaa') }}
from stg_staff_edorg_assignment_assoc seaa
where seaa.full_time_equivalency is not null
    and not (seaa.full_time_equivalency between 0 and 1)