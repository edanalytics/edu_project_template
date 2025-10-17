{{
  config(
    materialized="table",
    schema="build"
  )
}}

with stg_student as (
    select * from {{ ref('stg_ef3__students') }}
),
stu_edorg as (
    select * 
    from {{ ref('stg_ef3__student_education_organization_associations') }}
    where k_lea is not null
        and k_school is null
),
stu_races as (
    select * from {{ ref('bld_ef3__stu_race_ethnicity') }}
),
joined as (
    select
        stg_student.k_student,
        stg_student.k_student_xyear,
        stg_student.tenant_code,
        stg_student.api_year as school_year,
        stu_edorg.ed_org_id,
        stg_student.first_name,
        stg_student.middle_name,
        stg_student.last_name,
        {# stu_display_name logic: prefer SQL from this dbt variable, but default to "concat(...)" #}
        {{ var('edu:stu_demos:display_name_sql',
          "concat(
            stg_student.last_name, ', ',
            stg_student.first_name,
            coalesce(' ' || left(stg_student.middle_name, 1), '')
            )"
          )
        }} as display_name,
        concat(display_name, ' (', stg_student.student_unique_id, ')') as safe_display_name,
        stg_student.birth_date,
        stu_edorg.gender,
        stu_races.race_ethnicity,
        stu_races.race_array,
        stu_races.has_hispanic_latino_ethnicity,
        stu_edorg.k_lea,
        stu_edorg.lep_code
    from stg_student
    join stu_edorg
        on stg_student.k_student = stu_edorg.k_student
    left join stu_races
        on stu_edorg.k_student = stu_races.k_student
        and stu_edorg.ed_org_id = stu_races.ed_org_id
)
select * from joined
