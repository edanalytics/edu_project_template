{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3500 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
stg_student_academic_records as (
    select * from {{ ref('stg_ef3__student_academic_records_orig') }} sar
    where exists (
        select 1
        from brule
        where cast(sar.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    )
),
stg_student_academic_records__diplomas as (
    select * from {{ ref('stg_ef3__student_academic_records__diplomas') }} sard
)
/* Diploma Award Date must be within the school year begin and end date. */
select sar.k_student_academic_record, sar.k_student, sar.k_lea, sar.k_school, sar.ed_org_id,
    sar.school_year, sar.student_unique_id, sar.academic_term,
    sard.diploma_type, sard.diploma_description,
    s.state_student_id as legacy_state_student_id,
    brule.tdoe_error_code as error_code,
    concat('Diploma Award Date does not fall within the school year for Student ', 
        sar.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
        'Value Received: ', 
        sard.diploma_award_date, '. The state school year starts ',
        concat((cast(sar.school_year as int)-1), '-07-01'), ' and ends ', concat(cast(sar.school_year as int), '-06-30'), '.') as error,
    brule.tdoe_severity as severity
from stg_student_academic_records sar
join stg_student_academic_records__diplomas sard
    on sard.k_student_academic_record = sar.k_student_academic_record
join {{ ref('stg_ef3__students') }} s
    on s.k_student = sar.k_student
join brule
    on sar.school_year between brule.error_school_year_start and brule.error_school_year_end
where not(sard.diploma_award_date between to_date(concat(cast(sar.school_year as int)-1, '-07-01'), 'yyyy-MM-dd') 
        and to_date(concat(cast(sar.school_year as int), '-06-30'), 'yyyy-MM-dd'))