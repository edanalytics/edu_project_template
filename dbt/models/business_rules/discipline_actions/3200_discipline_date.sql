{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3200 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
stg_discipline_actions as (
    select *
           ,{{ edu_edfi_source.extract_descriptor('value:disciplineDescriptor::string') }} as discipline_action
    from {{ ref('stg_ef3__discipline_actions_orig') }} da
         , lateral variant_explode(v_disciplines)
    where exists (
        select 1
        from brule
        where cast(da.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    )
)
/* Discipline Date must fall within the school year. */
select da.k_student, da.k_school__responsibility, da.school_year,
    da.discipline_action_id, da.discipline_date, da.responsibility_school_id,
    da.student_unique_id,
    s.state_student_id as legacy_state_student_id,
    brule.tdoe_error_code as error_code,
    concat('Discipline Date for Student ', 
        da.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
        'does not fall within the school year. Value Received: ', da.discipline_date, '. The state school year starts ',
      concat(cast((da.school_year-1) as int), '-07-01'), ' and ends ', concat(cast(da.school_year as int), '-06-30'), '.') as error,
    brule.tdoe_severity as severity
    ,discipline_action
from stg_discipline_actions da
join {{ ref('stg_ef3__students') }} s
    on s.k_student = da.k_student
join brule
    on da.school_year between brule.error_school_year_start and brule.error_school_year_end
where not(da.discipline_date between to_date(concat(cast((da.school_year-1) as int), '-07-01'), 'yyyy-MM-dd') 
          and to_date(concat(cast(da.school_year as int), '-06-30'), 'yyyy-MM-dd'))
      and discipline_action IN ('I','S')
      and discipline_action NOT IN ('R','E')