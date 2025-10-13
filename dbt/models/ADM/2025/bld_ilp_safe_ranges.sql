{{
  config(
    materialized="table",
    schema="stg_adm"
  )
}}

/*
The purpose of this table is to un-overlap any ILP records that exist.
Hopefully, in the future, we won't have any because the data will get cleaned up, but we have it right now.
*/

with ilp_statuses as (
    select ilp.k_student, ilp.k_school, ilp.school_year, ilp.tenant_code, ilp.api_year,
        ilp.student_unique_id, ilp.ed_org_id,
        {{ edu_edfi_source.extract_descriptor('exploded_status.value:participationStatusDescriptor::string') }} as participation_status,
        exploded_status.value:statusBeginDate::date as status_begin_date,
        exploded_status.value:statusEndDate::date as status_end_date,
        ilp.v_ext:tdoe:totalYearsESL::int as total_years_esl
    from {{ ref('stg_ef3__student_language_instruction_program_associations') }} ilp,
    lateral variant_explode(ilp.v_program_participation_statuses) as exploded_status
    where ilp.program_name = 'ILP'
        and ilp.k_lea is null
),
rank_ilp_statuses as (
    select k_student, k_school, school_year, tenant_code, api_year, student_unique_id, ed_org_id,
        participation_status,
        status_begin_date,
        status_end_date,
        total_years_esl,
        row_number() over (
            partition by k_student, k_school, school_year, tenant_code, status_begin_date
            order by 
                case participation_status
                    when 'W' then 1
                    when 'L' then 2
                    when '1' then 3
                    when '2' then 4
                end
        ) as rn
    from ilp_statuses
),
ordered_ilp_statuses as (
    select k_student, k_school, school_year, tenant_code, api_year, student_unique_id, ed_org_id,
        participation_status,
        status_begin_date,
        status_end_date,
        total_years_esl,
        lead(status_begin_date) over (
            partition by k_student, k_school, school_year, tenant_code
            order by status_begin_date) as next_status_begin_date
    from rank_ilp_statuses 
    where rn = 1
)
select k_student, k_school, school_year, tenant_code, api_year, student_unique_id, ed_org_id,
    participation_status,
    status_begin_date,
    status_end_date,
    total_years_esl,
    case
        when next_status_begin_date is not null and next_status_begin_date < status_begin_date then date_sub(next_status_begin_date, 1)
        else coalesce(status_end_date, to_date(concat(school_year, '-06-30', 'yyyy-MM-dd')))
    end as safe_status_end_date,
    row_number() over (
        partition by k_student, k_school, school_year, tenant_code
        order by status_begin_date
    ) as seq
from ordered_ilp_statuses