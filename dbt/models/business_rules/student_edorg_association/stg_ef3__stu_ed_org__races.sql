with stage_stu_ed_org as (
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
        {{ edu_edfi_source.extract_descriptor('value:raceDescriptor::string') }} as race
    from stage_stu_ed_org
        , lateral variant_explode(v_races)
)
select * from flattened

