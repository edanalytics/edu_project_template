{{
  config(
    materialized="table",
    schema="wh",
    post_hook=[
        "alter table {{ this }} alter column k_student set not null",
        "alter table {{ this }} alter column k_lea set not null",
        "alter table {{ this }} add primary key (k_student, k_lea)",
    ]
  )
}}

with stg_student as (
    select * from {{ ref('edu_edfi_source', 'stg_ef3__students') }}
),
stu_immutable_demos as (
    select * from {{ ref('bld_ef3__immutable_stu_demos_by_district') }}
),
stu_ids as (
    select * from {{ ref('edu_wh', 'bld_ef3__wide_ids_student') }}
),
stu_chars as (
    select * from {{ ref('edu_wh', 'bld_ef3__student_characteristics') }}
),
stu_indicators as (
    select * from {{ ref('edu_wh', 'bld_ef3__student_indicators') }}
),
stu_languages as (
    select tenant_code, api_year, k_student, ed_org_id, code_value as calc_home_language
    from {{ ref('stg_ef3__stu_ed_org__languages') }} x
    where k_lea is not null
        and k_school is null
        and upper(language_use) in ('NATIVE LANGUAGE', 'DOMINANT LANGUAGE', 'HOME LANGUAGE')
    qualify 1 = row_number() over (
        partition by tenant_code, api_year, k_student, ed_org_id
        order by 
            case upper(language_use)
                when 'NATIVE LANGUAGE' then 1
                when 'HOME LANGUAGE' then 2
                when 'DOMINANT LANGUAGE' then 3
            end
    )
),
formatted as (
    select
        stg_student.k_student,
        stg_student.k_student_xyear,
        stu_immutable_demos.k_lea,
        stg_student.tenant_code,
        stg_student.api_year as school_year,
        stg_student.student_unique_id,
        stu_immutable_demos.ed_org_id,
        -- student ids
        {{ edu_wh.accordion_columns(
            source_table='bld_ef3__wide_ids_student',
            exclude_columns=['tenant_code', 'api_year', 'k_student', 'k_student_xyear', 'ed_org_id'],
            source_alias='stu_ids'
        ) }}

        stu_immutable_demos.lep_code,
        stu_immutable_demos.gender,
        stu_immutable_demos.race_ethnicity,
        stu_immutable_demos.has_hispanic_latino_ethnicity,

        -- student characteristics
        {{ edu_wh.accordion_columns(
            source_table='bld_ef3__student_characteristics',
            exclude_columns=['tenant_code', 'api_year', 'k_student', 'k_student_xyear', 'ed_org_id'],
            source_alias='stu_chars',
            coalesce_value = 'FALSE'
        ) }}

        -- student indicators
        {{ edu_wh.accordion_columns(
            source_table='bld_ef3__student_indicators',
            exclude_columns=['tenant_code', 'api_year', 'k_student', 'k_student_xyear', 'ed_org_id'],
            source_alias='stu_indicators'
        ) }}

        stu_immutable_demos.race_array,
        stu_immutable_demos.safe_display_name,
        stu_languages.calc_home_language

    from stg_student
    join stu_immutable_demos
        on stu_immutable_demos.k_student = stg_student.k_student
    left join stu_ids
        on stu_immutable_demos.k_student = stu_ids.k_student
        and stu_immutable_demos.ed_org_id = stu_ids.ed_org_id
    left join stu_chars
        on stu_immutable_demos.k_student = stu_chars.k_student
        and stu_immutable_demos.ed_org_id = stu_chars.ed_org_id
    left join stu_indicators
        on stu_immutable_demos.k_student = stu_indicators.k_student
        and stu_immutable_demos.ed_org_id = stu_indicators.ed_org_id
    left join stu_languages
        on stu_immutable_demos.k_student = stu_languages.k_student
        and stu_immutable_demos.ed_org_id = stu_languages.ed_org_id
)

select * from formatted
order by tenant_code, school_year desc, k_student, ed_org_id