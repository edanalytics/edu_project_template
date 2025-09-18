{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 2011 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
bs_class_periods as (
    select bs.k_bell_schedule, bs.k_bell_schedule, bs.k_school, bs.school_year, bs.tenant_code,
        cp.k_class_period, cp.class_period_name, cp.class_period_school_id
    from {{ ref('stg_ef3__bell_schedules_orig') }} bs
    join {{ ref('stg_ef3__bell_schedules__class_periods') }} cp
        on cp.k_bell_schedule = bs.k_bell_schedule
        and cp.k_school = bs.k_school
    where exists (
        select 1
        from brule
        where cast(bs.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    )
),
cp_no_bs as (
    select *
    from {{ ref('stg_ef3__class_periods_orig') }} cp
    where not exists (
        select 1
        from bs_class_periods bs
        where bs.k_class_period = cp.k_class_period
            and bs.k_school = cp.k_school
            and bs.tenant_code = cp.tenant_code
    )
)
/* Class Periods must be tied to a Bell Schedule. */
select cp.k_class_period, cast(cp.school_year as int) as school_year, cp.class_period_name, cp.school_id,
    brule.tdoe_error_code as error_code,
    concat('Class Period ', cp.class_period_name, ' must be tied to a Bell Schedule.') as error,
    brule.tdoe_severity as severity
from cp_no_bs cp
join brule
    on cp.school_year between brule.error_school_year_start and brule.error_school_year_end

