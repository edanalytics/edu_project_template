{{
  config(
    materialized="table",
    schema="stg_adm"
  )
}}

/*
The purpose of this table is to un-overlap any SPED records that exist.
*/

with sped_options as (
    select sped.k_student, sped.k_lea, sped.school_year, sped.tenant_code, sped.api_year,
        sped.student_unique_id, sped.ed_org_id, 
        split_part(exploded_services.value:specialEducationProgramServiceDescriptor::string, '#', -1) as participation_status,
        cast(regexp_extract(split_part(exploded_services.value:specialEducationProgramServiceDescriptor::string, '#', -1), 'Option (\\d+)', 1) as int) as option,
        exploded_services.value:primaryIndicator::boolean as primary_indicator,
        exploded_services.value:serviceBeginDate::date as service_begin_date,
        exploded_services.value:serviceEndDate::date as service_end_date,
        sped.v_ext:tdoe:serviceEligibilityDate::date as service_eligibility_date
    from {{ ref('stg_ef3__student_special_education_program_associations') }} sped,
    lateral variant_explode(sped.v_special_education_program_services) as exploded_services
    where sped.k_school is null
        and sped.program_name = 'Special Education'
),
rank_sped_options as (
    select k_student, k_lea, school_year, tenant_code, api_year, student_unique_id, ed_org_id,
        participation_status, option,
        primary_indicator, service_begin_date, service_end_date, service_eligibility_date,
        row_number() over (
            partition by k_student, k_lea, school_year, tenant_code, primary_indicator, service_begin_date
            order by option desc
        ) as rn
    from sped_options
),
ordered_sped_options as (
    select k_student, k_lea, school_year, tenant_code, api_year, student_unique_id, ed_org_id,
        participation_status, option,
        primary_indicator, service_begin_date, service_end_date, service_eligibility_date,
        lead(service_begin_date) over (
            partition by k_student, k_lea, school_year, tenant_code, primary_indicator
            order by service_begin_date) as next_service_begin_date
    from rank_sped_options 
    where rn = 1
)
select k_student, k_lea, school_year, tenant_code, api_year, student_unique_id, ed_org_id,
    participation_status, option,
    primary_indicator, service_begin_date, service_end_date, service_eligibility_date,
    case
        when next_service_begin_date is not null and next_service_begin_date < service_begin_date then date_sub(next_service_begin_date, 1)
        else coalesce(service_end_date, to_date(concat(school_year, '-06-30', 'yyyy-MM-dd')))
    end as safe_service_end_date
from ordered_sped_options