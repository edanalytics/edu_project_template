{{
  config(
    materialized="table",
    schema="stg_adm"
  )
}}

/*
The purpose of this table is to un-overlap any ILPD records that exist.
Hopefully, in the future, we won't have any because the data will get cleaned up, but we have it right now.
*/

with ilpd_statuses as (
    select ilpd.k_student, ilpd.k_school, ilpd.school_year, ilpd.tenant_code, ilpd.api_year,
        ilpd.student_unique_id, ilpd.ed_org_id,
        exploded_program.value:serviceBeginDate::date as service_begin_date,
        exploded_program.value:serviceEndDate::date as service_end_date
    from {{ ref('stg_ef3__student_language_instruction_program_associations') }} ilpd,
    lateral variant_explode(ilpd.v_language_instruction_program_services) as exploded_program
    where ilpd.program_name = 'ILPD'
        and ilpd.k_lea is null
),
clean_ilpd_statuses as (
    select k_student, k_school, school_year, tenant_code, api_year, student_unique_id, ed_org_id,
        service_begin_date,
        service_end_date,
        lead(service_begin_date) over (
            partition by k_student, k_school, school_year, tenant_code
            order by service_begin_date) as next_service_begin_date
    from ilpd_statuses 
)
select ilpd.k_student, ilpd.k_school, ilpd.school_year, ilpd.tenant_code, ilpd.api_year, ilpd.student_unique_id, ilpd.ed_org_id,
    ilpd.service_begin_date,
    ilpd.service_end_date,
    case
        when ilpd.next_service_begin_date is not null then date_sub(ilpd.next_service_begin_date, 1)
        else coalesce(ilpd.service_end_date, to_date(concat(ilpd.school_year, '06-30', 'yyyy-MM-dd')))
    end as safe_service_end_date
from clean_ilpd_statuses ilpd