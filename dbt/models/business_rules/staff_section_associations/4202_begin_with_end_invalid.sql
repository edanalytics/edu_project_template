{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 4202 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
stg_staff_section_associations as (
    select * from {{ ref('stg_ef3__staff_section_associations_orig') }} ssa
    where ssa.end_date is not null
        and exists (
        select 1
        from brule
        where cast(ssa.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    )
)
/* End Date must be greater than Begin Date. */
select ssa.k_staff, ssa.k_course_section, ssa.local_course_code, ssa.school_year, ssa.school_id, 
    ssa.section_id, ssa.session_name, ssa.staff_unique_id, ssa.begin_date,
    brule.tdoe_error_code as error_code,
    concat('Staff Section Association End Date must be greater than the Begin Date. End Date received: ',
        ssa.end_date, ', Begin Date: ', ssa.end_date, '.') as error,
    brule.tdoe_severity as severity
from stg_staff_section_associations ssa
join brule
    on ssa.school_year between brule.error_school_year_start and brule.error_school_year_end
where ssa.end_date < ssa.begin_date