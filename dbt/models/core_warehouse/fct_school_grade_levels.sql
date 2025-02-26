{{
  config(
    materialized="table",
    schema="wh",
    post_hook=[
        "alter table {{ this }} alter column k_school set not null",
        "alter table {{ this }} alter column grade_level set not null",
        "alter table {{ this }} add primary key (k_school, grade_level)"
    ]
  )
}}

with stg_school_grade_levels as (
    select * from {{ ref('stg_ef3__schools__grade_levels') }}
),
formatted as (
    select sgl.tenant_code, sgl.k_school, sgl.grade_level,
        CASE 
            WHEN sgl.grade_level IN ('P3', 'P4', 'Prekindergarten') THEN 'PK'
            WHEN sgl.grade_level = 'Kindergarten' THEN 'K'
            WHEN sgl.grade_level = 'First grade' THEN '1'
            WHEN sgl.grade_level = 'Second grade' THEN '2'
            WHEN sgl.grade_level = 'Third grade' THEN '3'
            WHEN sgl.grade_level = 'Fourth grade' THEN '4'
            WHEN sgl.grade_level = 'Fifth grade' THEN '5'
            WHEN sgl.grade_level = 'Sixth grade' THEN '6'
            WHEN sgl.grade_level = 'Seventh grade' THEN '7'
            WHEN sgl.grade_level = 'Eighth grade' THEN '8'
            WHEN sgl.grade_level = 'Ninth grade' THEN '9'
            WHEN sgl.grade_level = 'Tenth grade' THEN '10'
            WHEN sgl.grade_level = 'Eleventh grade' THEN '11'
            WHEN sgl.grade_level = 'Twelfth grade' THEN '12'
            ELSE NULL
        END AS grade_level_short,
        x.grade_level_integer
    from stg_school_grade_levels sgl
    join teds_dev.dev_smckee_seed.xwalk_grade_levels x
        on upper(x.grade_level) = upper(sgl.grade_level)
)
select * from formatted
order by tenant_code, k_school
