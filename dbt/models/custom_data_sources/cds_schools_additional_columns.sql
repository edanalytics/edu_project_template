{{
  config(
    materialized="table",
    schema="cds"
  )
}}

with grade_level_range as (
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
)
select s.tenant_code, s.k_school, {{ get_school_from_school_id('s.school_id') }} as school_number,
    glr.grade_levels
from {{ ref('stg_ef3__schools') }} s
left outer join grade_level_range glr
    on glr.tenant_code = s.tenant_code
    and glr.k_school = s.k_school