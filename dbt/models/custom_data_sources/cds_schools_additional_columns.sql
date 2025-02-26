{{
  config(
    materialized="table",
    schema="cds"
  )
}}

select distinct sgl.tenant_code, sgl.k_school, 
    concat(
        first(sgl.grade_level_short) over (
            partition by sgl.tenant_code, sgl.k_school
            order by sgl.grade_level_integer
            rows between unbounded preceding and unbounded following),
        '-',
        last(sgl.grade_level_short) over (
            partition by sgl.tenant_code, sgl.k_school
            order by sgl.grade_level_integer
            rows between unbounded preceding and unbounded following)
    ) as grade_levels
from {{ ref('fct_school_grade_levels') }} sgl
where sgl.grade_level_short is not null