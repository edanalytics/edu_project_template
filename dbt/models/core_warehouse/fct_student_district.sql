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

{{ edu_wh.cds_depends_on('tdoe:fct_student_district:custom_data_sources') }}
{% set custom_data_sources = var('tdoe:fct_student_district:custom_data_sources', []) %}

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
        stu_immutable_demos.safe_display_name

        -- custom indicators
        {{ edu_wh.add_cds_columns(custom_data_sources=custom_data_sources) }}

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

    -- custom data sources
    {{ edu_wh.add_cds_joins_v2(custom_data_sources=custom_data_sources) }}
)

select * from formatted
order by tenant_code, school_year desc, k_student, ed_org_id