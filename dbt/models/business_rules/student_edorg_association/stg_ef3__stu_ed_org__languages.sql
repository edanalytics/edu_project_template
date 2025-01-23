{{
  config(
    materialized="table",
    schema="stage"
  )
}}

with stg_stu_ed_org as (
    select * from {{ ref('stg_ef3__student_education_organization_associations_orig') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_student,
        k_student_xyear,
        ed_org_id,
        k_lea,
        k_school,
        {{ edu_edfi_source.extract_descriptor('lang_uses.value:languageUseDescriptor::string') }} as language_use,
        {{ edu_edfi_source.extract_descriptor('lang.value:languageDescriptor::string') }} as code_value
    from stg_stu_ed_org
        , lateral variant_explode(v_languages) as lang
        , lateral variant_explode(lang.value:uses) as lang_uses
)
select * from flattened