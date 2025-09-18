{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3109 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
stg_homeless as (
    select * 
    from {{ ref('stg_ef3__student_homeless_program_associations_orig') }} sh
    where sh.k_lea is not null
        and exists (
        select 1
        from brule
        where cast(sh.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    )
)
/* Homeless Students must have a Primary Nighttime Residence. */
select h.k_student, h.k_program, h.k_lea, h.school_year, h.student_unique_id, h.ed_org_id, h.program_enroll_begin_date,
    s.state_student_id as legacy_state_student_id,
    brule.tdoe_error_code as error_code,
    concat('Homeless Primary Nighttime Residence for Student ', 
        h.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
        'cannot be blank.') as error,
    brule.tdoe_severity as severity
from stg_homeless h
join {{ ref('stg_ef3__students') }} s
    on s.k_student = h.k_student
join brule
    on h.school_year between brule.error_school_year_start and brule.error_school_year_end
where h.homeless_primary_nighttime_residence is null