{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 2011 %}

with bs_class_periods as (
    select bs.k_bell_schedule, bs.k_bell_schedule, bs.k_school, bs.school_year, bs.tenant_code,
        cp.k_class_period, cp.class_period_name, cp.class_period_school_id
    from {{ ref('stg_ef3__bell_schedules_orig') }} bs
    join {{ ref('stg_ef3__bell_schedules__class_periods') }} cp
        on cp.k_bell_schedule = bs.k_bell_schedule
        and cp.k_school = bs.k_school
    where 1=1
        {{ school_year_exists(error_code, 'bs') }}
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
select cp.k_class_period, cp.class_period_name, cp.school_id,
    {{ error_code }} as error_code,
    'Class Period must be tied to a Bell Schedule.' as error,
    {{ error_severity_column(error_code, 'cp') }}
from cp_no_bs cp

